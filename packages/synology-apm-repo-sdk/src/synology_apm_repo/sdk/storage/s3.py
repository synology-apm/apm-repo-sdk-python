"""``S3Store`` — an ``ObjectStore`` implementation for S3-compatible object
storage, one of the two backends this project's own object-storage samples
are laid out for.

``aioboto3``/``botocore`` are imported lazily, inside ``__init__`` and each
method — a substantial import graph, and ``storage/__init__.py`` imports
this module unconditionally, so a module-level import would make every
caller of ``storage`` pay that cost even if it never touches ``S3Store``.
Python caches the import after the first call, so the repeated ``import``
statements below cost a dict lookup, not a re-import.

``aioboto3``'s client is an async context manager, sitting on ``aiohttp``
(many outstanding round-trips overlap on one thread); this class creates
it lazily on first use and owns its teardown via ``S3Store.aclose``
(called by ``Session.close()``) — forgetting that would leak the
connector. The constructor itself stays synchronous.

S3 has no directory entities: ``S3Store.read`` treats an out-of-range
``Range`` request as the ``ObjectStore`` contract's short-read-at-EOF case
(``b""``) rather than S3's own ``InvalidRange`` client error, matching
every other backend.

Every client this module builds goes through ``_with_default_timeouts``,
which caps botocore's batch-job-tuned timeouts to values an interactive
caller can actually wait through.
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

# botocore's own defaults (60s connect, 60s read) are tuned for a batch job,
# not an interactive reachability probe. These apply unless the caller
# already passed its own `config`, which wins field-by-field over these.
_DEFAULT_CONNECT_TIMEOUT = 5
_DEFAULT_READ_TIMEOUT = 15
_DEFAULT_MAX_ATTEMPTS = 2

# botocore's own default (10) flows into aiohttp.TCPConnector(limit=...),
# capping the total concurrent connections one client can hold open across
# every caller sharing it — including chunk_walk.py's max_concurrent_reads/
# max_concurrent_opens, which would otherwise queue inside the connector
# itself. Raised well above any concurrency this SDK exposes today, since
# one client is shared across a whole session's unrelated callers, not
# scoped to one export call.
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
            # A cancellation mid-body-read must not let this connection
            # quietly return to aiohttp's pool with the rest of the old
            # response unread on the wire — close() discards it outright
            # (a new connection opens next time) instead of risking reuse
            # of a desynced one.
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
