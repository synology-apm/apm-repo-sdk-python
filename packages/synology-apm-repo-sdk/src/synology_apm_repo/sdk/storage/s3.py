"""``S3Store`` — a real, executable ``ObjectStore`` implementation for
S3-compatible object storage, one of the two backends this project's own
object-storage samples are laid out for.

``aioboto3``/``botocore`` are imported lazily, inside ``__init__`` and each
method, rather than at module scope: ``aioboto3`` is always installed (a
required dependency), but it's a substantial import graph (botocore,
aiohttp, ...), and this module is imported unconditionally by
``storage/__init__.py``, so a module-level import would make every caller
of ``storage`` pay that cost even if it never touches ``S3Store``. Python
caches the import after the first successful call, so the repeated
``import`` statements below cost a dict lookup, not a re-import.

This and ``AzureStore`` use ``aioboto3``/async rather than a thread around
plain ``boto3`` because both sit on ``aiohttp``, where many outstanding
network round-trips genuinely overlap on one thread — a real gain a
thread-pool wrapper around a synchronous client wouldn't provide. One
consequence: ``aioboto3``'s client is an *async context manager*, so this
class creates it lazily on first use and owns its teardown via
``S3Store.aclose`` (called by ``Session.close()``) — forgetting that would
leak an ``aiohttp`` connector. The constructor itself stays synchronous
(it only builds an ``aioboto3.Session``, which does no I/O).

Two contract differences from ``LocalFsStore``, both because S3 genuinely
has no directory entities (``listdir``'s and ``exists``'s own docstrings
cover the "directory" side of that): ``S3Store.read`` treats an
out-of-range ``Range`` request as the ``ObjectStore`` contract's own
short-read-at-EOF case (``b""``) rather than S3's own ``InvalidRange``
client error, to match the same contract every other backend already
provides.

Every client this module builds goes through ``_with_default_timeouts``,
which caps botocore's own long batch-job timeouts down to values an
interactive caller — the TUI's connect dialog, in particular — can actually
wait through when an endpoint is unreachable.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any

from ..errors import NotFoundError
from .base import backend_key as _key
from .prefix_listing import as_list_prefix, exists_via_prefix_probe, sorted_relative_names

if TYPE_CHECKING:
    from botocore.exceptions import ClientError

_NOT_FOUND_ERROR_CODES = frozenset({"NoSuchKey", "404", "NotFound"})

# botocore's own defaults (60s connect, 60s read, plus several retries on a
# connect timeout) are tuned for long-running batch jobs, not an interactive
# "is this endpoint even reachable" probe — left alone, an unreachable or
# black-holed endpoint can block a caller for minutes. These apply to every
# client this module builds unless the caller already passed its own
# ``config``, in which case only the fields left unset there fall back to
# these.
_DEFAULT_CONNECT_TIMEOUT = 5
_DEFAULT_READ_TIMEOUT = 15
_DEFAULT_MAX_ATTEMPTS = 2

# botocore's own default (10, botocore.config.Config.max_pool_connections)
# flows straight into aiohttp.TCPConnector(limit=...), meaning it caps the
# *total* concurrent connections one S3Store's client
# can ever hold open, across every caller sharing it: chunk_walk.py's
# max_concurrent_reads (one semaphore sized to it gates both the
# cross-bucket dispatch loop and each bucket's own in-bucket fan-out) and
# max_concurrent_opens both ultimately compete for slots in
# this one pool, and either knob raised past this ceiling on its own (let
# alone the two together) would queue inside the connector itself before a
# single byte moves, on top of whatever the network/server itself is
# doing — a silent, easy-to-miss
# second bottleneck neither knob's docstring accounts for. Raised
# generously above any concurrency this SDK exposes today (their sum
# rarely exceeds double digits) rather than tied to a specific caller's
# setting, since one client is shared across a whole session's unrelated
# callers (interactive browsing, a concurrent verify, ...), not scoped to
# one export call.
_DEFAULT_MAX_POOL_CONNECTIONS = 32


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _with_default_timeouts(client_kwargs: dict[str, Any]) -> dict[str, Any]:
    """``client_kwargs`` with an interactive-friendly ``config`` merged in
    (values and rationale: the module-level constants above).
    ``Config.merge()`` lets a ``config`` the caller already supplied take
    precedence field-by-field over these defaults, rather than replacing
    it outright."""
    from botocore.config import Config

    default_config = Config(
        connect_timeout=_DEFAULT_CONNECT_TIMEOUT,
        read_timeout=_DEFAULT_READ_TIMEOUT,
        retries={"max_attempts": _DEFAULT_MAX_ATTEMPTS},
        max_pool_connections=_DEFAULT_MAX_POOL_CONNECTIONS,
    )
    merged = dict(client_kwargs)
    user_config = merged.get("config")
    merged["config"] = default_config.merge(user_config) if user_config is not None else default_config
    return merged


def _import_aioboto3() -> Any:
    """Lazy ``import aioboto3``, same rationale as the module docstring
    above. Shared by every constructor/free function here that needs it,
    so the choice of what to import lives in one place."""
    import aioboto3

    return aioboto3


class S3Store:
    """An S3 (or S3-compatible — MinIO, Synology C2, ...) bucket, addressed
    by ``"/"``-separated paths relative to the bucket root.

    ``client``, if given, is used as-is (tests inject a fake or otherwise
    pre-configured *async* client this way, already entered); otherwise
    one is built lazily on first use from ``client_kwargs``
    (``region_name``, ``endpoint_url`` — for S3-compatible but non-AWS
    backends, ``aws_access_key_id``/``aws_secret_access_key``, ...), passed
    straight through to ``aioboto3.Session().client("s3", **client_kwargs)``.
    """

    def __init__(
        self,
        bucket: str,
        *,
        client: Any = None,
        **client_kwargs: Any,
    ) -> None:
        self._bucket = bucket
        self._client_kwargs = client_kwargs
        self._client: Any = client
        self._owns_client = client is None
        self._stack: AsyncExitStack | None = None
        self._client_lock = asyncio.Lock()
        self._session: Any = None
        if client is None:
            aioboto3 = _import_aioboto3()
            # Session construction is pure configuration, no I/O.
            self._session = aioboto3.Session()

    def __repr__(self) -> str:
        return f"S3Store(bucket={self._bucket!r})"

    async def _get_client(self) -> Any:
        """The live async client, created on first use.

        Double-checked under an ``asyncio.Lock`` because entering the client
        context is itself an ``await`` — two Tasks racing here without it
        would each build a connector and one would be leaked outright.
        """
        if self._client is not None:
            return self._client
        async with self._client_lock:
            if self._client is None:
                stack = AsyncExitStack()
                self._client = await stack.enter_async_context(
                    self._session.client("s3", **_with_default_timeouts(self._client_kwargs)),
                )
                self._stack = stack
        return self._client

    async def aclose(self) -> None:
        """Release the underlying ``aiohttp`` connector.

        Required, not optional, for a client this class created itself. An
        injected ``client`` is left alone: whoever created it owns closing
        it. Safe to call more than once.
        """
        stack, self._stack = self._stack, None
        if stack is not None:
            self._client = None
            await stack.aclose()

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        from botocore.exceptions import ClientError

        client = await self._get_client()
        key = _key(path)
        range_header = f"bytes={offset}-{offset + length - 1}" if length is not None else f"bytes={offset}-"
        try:
            response = await client.get_object(Bucket=self._bucket, Key=key, Range=range_header)
        except ClientError as exc:
            code = _error_code(exc)
            if code == "InvalidRange":
                # offset is at or past the object's real size - the same
                # "short read at EOF" the ObjectStore contract already
                # requires of every backend, just signaled differently here.
                return b""
            if code in _NOT_FOUND_ERROR_CODES:
                raise NotFoundError("no such object", ref=key) from exc
            raise
        body = response["Body"]
        try:
            return await body.read()  # type: ignore[no-any-return]
        except asyncio.CancelledError:
            # A cancellation landing mid-body-read must not let this
            # partially-read response's connection quietly return to
            # aiohttp's connection pool: a connection released mid-stream
            # still has the rest of the old response body sitting unread
            # on the wire, and the next request to reuse it from the pool
            # would desync, reading stale bytes as if they were its own
            # response (or worse) rather than a fresh one. ``close()``
            # (synchronous — StreamingBody proxies straight through to the
            # wrapped ``aiohttp.ClientResponse``, whose own ``close()``
            # explicitly discards the connection instead of releasing it)
            # forces that instead, at the cost of this one connection (a
            # new one is opened on the next request) rather than risking
            # a corrupted one silently reused.
            body.close()
            raise

    async def size(self, path: str) -> int:
        from botocore.exceptions import ClientError

        client = await self._get_client()
        key = _key(path)
        try:
            response = await client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if _error_code(exc) in _NOT_FOUND_ERROR_CODES:
                raise NotFoundError("no such object", ref=key) from exc
            raise
        return int(response["ContentLength"])

    async def exists(self, path: str) -> bool:
        """``True`` for either an object exactly at ``path``, or a
        "directory" — a prefix with at least one object under it.
        ``ObjectStore.exists()``'s own contract explicitly covers both
        (``layout.py`` relies on it: ``db``/``@data`` are never
        objects in their own right on an object-storage backend, only
        prefixes with real objects underneath — a ``head_object`` check
        alone would wrongly report every such "directory" as absent)."""
        from botocore.exceptions import ClientError

        client = await self._get_client()
        key = _key(path)
        list_prefix = as_list_prefix(key)

        async def _probe_prefix() -> bool:
            response = await client.list_objects_v2(Bucket=self._bucket, Prefix=list_prefix, MaxKeys=1)
            return bool(response.get("Contents")) or bool(response.get("KeyCount", 0))

        return await exists_via_prefix_probe(
            head=lambda: client.head_object(Bucket=self._bucket, Key=key),
            error_type=ClientError,
            is_not_found=lambda exc: _error_code(exc) in _NOT_FOUND_ERROR_CODES,
            probe_prefix=_probe_prefix,
        )

    async def listdir(self, path: str) -> list[str]:
        """An absent "directory" (a prefix with zero objects under it) and
        an empty one are indistinguishable — S3 has no real directory
        entities, only prefixes — so both correctly report ``[]`` rather
        than one of them raising ``NotFoundError``."""
        client = await self._get_client()
        prefix = _key(path)
        list_prefix = as_list_prefix(prefix)
        raw_names: list[str] = []
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=self._bucket, Prefix=list_prefix, Delimiter="/"):
            raw_names.extend(common_prefix["Prefix"] for common_prefix in page.get("CommonPrefixes", []))
            raw_names.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted_relative_names(raw_names, list_prefix)


async def list_buckets(**client_kwargs: Any) -> list[str]:
    """Every bucket visible to these credentials — a bucket-*less* operation
    ``S3Store`` has no method for, since all four of its methods are scoped
    to one chosen bucket. Builds its own transient client the same
    lazy-import way ``S3Store`` does and closes it before returning; there
    is no lifecycle for a caller to manage beyond this one call.

    ``client_kwargs`` is exactly what a caller would otherwise pass to
    ``S3Store``. Raises whatever the underlying ``aioboto3`` call raises
    (e.g. ``ClientError`` for ``AccessDenied`` when the credentials aren't
    authorized to list buckets at the account level) — unhandled, the same
    as every other backend-specific exception ``S3Store``'s own methods let
    propagate.
    """
    aioboto3 = _import_aioboto3()
    session = aioboto3.Session()
    async with session.client("s3", **_with_default_timeouts(client_kwargs)) as client:
        response = await client.list_buckets()
    return sorted(bucket["Name"] for bucket in response.get("Buckets", []))
