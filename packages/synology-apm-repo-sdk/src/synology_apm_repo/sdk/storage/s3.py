"""``S3Store`` — an ``ObjectStore`` implementation for S3-compatible object
storage.

``aioboto3``/``botocore`` are imported lazily, inside ``__init__`` and each
method: ``storage/__init__.py`` imports this module unconditionally and
their import graph is large.

The ``aiohttp``-based client is created on first use; ``S3Store.close``
(called by ``Session.close()``) must release it or the connector leaks.
Every client gets ``_with_default_timeouts``' interactive-friendly config.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Iterator
from contextlib import AsyncExitStack, contextmanager
from typing import TYPE_CHECKING, Any, override

from .._util.closing import AsyncClosing
from ..errors import NotFoundError, PermissionDeniedError, StorageBackendError
from .base import NETWORK_CONNECT_TIMEOUT, NETWORK_READ_TIMEOUT, Entry
from .base import backend_key as _key
from .prefix_listing import (
    as_list_prefix,
    exists_via_prefix_probe,
    sorted_relative_entries,
)

if TYPE_CHECKING:
    from botocore.exceptions import ClientError

_NOT_FOUND_ERROR_CODES = frozenset({"NoSuchKey", "404", "NotFound"})
_ACCESS_DENIED_ERROR_CODES = frozenset(
    {"AccessDenied", "AllAccessDisabled", "Forbidden", "403", "InvalidAccessKeyId", "SignatureDoesNotMatch"}
)

# Total attempts per request, down from botocore's default for a batch job.
_DEFAULT_MAX_ATTEMPTS = 2

# The connection cap of one client (botocore's default is 10). One client
# serves a whole session's callers, so it sits well above any single
# caller's concurrency (e.g. an export's concurrent reads), which would
# otherwise queue inside the connector.
_DEFAULT_MAX_POOL_CONNECTIONS = 32


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


@contextmanager
def _mapped_errors(key: str) -> Iterator[None]:
    """Re-raise a ``botocore``/``aiohttp`` failure as ``ObjectStore``'s
    ``NotFoundError``/``PermissionDeniedError``/``StorageBackendError``."""
    import aiohttp
    from botocore.exceptions import BotoCoreError, ClientError

    try:
        yield
    except ClientError as exc:
        code = _error_code(exc)
        if code in _NOT_FOUND_ERROR_CODES:
            raise NotFoundError("no such object", ref=key) from exc
        if code in _ACCESS_DENIED_ERROR_CODES:
            raise PermissionDeniedError(f"access denied ({code})", ref=key) from exc
        raise StorageBackendError(f"S3 request failed ({code or exc})", ref=key) from exc
    except (BotoCoreError, aiohttp.ClientError, TimeoutError) as exc:
        raise StorageBackendError(f"S3 request failed: {exc}", ref=key) from exc


def _with_default_timeouts(client_kwargs: dict[str, Any]) -> dict[str, Any]:
    """``client_kwargs`` with the network timeouts and the constants above
    as its ``config``; a caller-supplied ``config`` wins field by field."""
    from botocore.config import Config

    default_config = Config(
        connect_timeout=NETWORK_CONNECT_TIMEOUT,
        read_timeout=NETWORK_READ_TIMEOUT,
        retries={"max_attempts": _DEFAULT_MAX_ATTEMPTS},
        max_pool_connections=_DEFAULT_MAX_POOL_CONNECTIONS,
    )
    merged = dict(client_kwargs)
    user_config = merged.get("config")
    merged["config"] = default_config.merge(user_config) if user_config is not None else default_config
    return merged


def _import_aioboto3() -> Any:
    """Lazy ``import aioboto3`` (see the module docstring)."""
    import aioboto3

    return aioboto3


@dataclasses.dataclass(frozen=True, slots=True)
class S3StoreDescriptor:
    """Picklable recipe for rebuilding an equivalent ``S3Store``."""

    bucket: str
    client_kwargs: dict[str, object]

    def build(self) -> S3Store:
        return S3Store(self.bucket, **self.client_kwargs)


class S3Store(AsyncClosing):
    """An S3 (or S3-compatible — MinIO, Synology C2, ...) bucket, addressed
    by ``"/"``-separated paths relative to the bucket root.

    ``client``, if given, is an already-entered async client used as-is
    and not closed here. Otherwise one is built on first use from
    ``client_kwargs`` (``region_name``, ``endpoint_url``,
    ``aws_access_key_id``/``aws_secret_access_key``, ...), passed to
    ``aioboto3.Session().client("s3", ...)``.
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

    def descriptor(self) -> S3StoreDescriptor | None:
        """How a worker process rebuilds this store; ``None`` for one built
        on an injected ``client``, which has no picklable recipe."""
        if not self._owns_client:
            return None
        return S3StoreDescriptor(self._bucket, dict(self._client_kwargs))

    @override
    def __repr__(self) -> str:
        return f"S3Store(bucket={self._bucket!r})"

    async def _get_client(self) -> Any:
        """The live async client, created on first use under a lock:
        entering the client awaits, and two racing tasks would otherwise
        each build one and leak a connector."""
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

    @override
    async def close(self) -> None:
        """Release the ``aiohttp`` connector of a client this store created
        (an injected one is left alone). Safe to call more than once."""
        stack, self._stack = self._stack, None
        if stack is not None:
            self._client = None
            await stack.aclose()

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        from botocore.exceptions import ClientError

        if length is not None and length <= 0:
            # No valid Range header exists for zero bytes; the object must
            # still exist, as for LocalFsStore.
            await self.size(path)
            return b""
        client = await self._get_client()
        key = _key(path)
        range_header = f"bytes={offset}-{offset + length - 1}" if length is not None else f"bytes={offset}-"
        with _mapped_errors(key):
            try:
                response = await client.get_object(Bucket=self._bucket, Key=key, Range=range_header)
            except ClientError as exc:
                if _error_code(exc) == "InvalidRange":
                    # offset at or past the end: ObjectStore's short read at EOF.
                    return b""
                raise
            body = response["Body"]
            try:
                return await body.read()  # type: ignore[no-any-return]
            except asyncio.CancelledError:
                # Discard the connection: returned to the pool with the rest of
                # the response unread, it would desync the next request.
                body.close()
                raise

    async def size(self, path: str) -> int:
        client = await self._get_client()
        key = _key(path)
        with _mapped_errors(key):
            response = await client.head_object(Bucket=self._bucket, Key=key)
        return int(response["ContentLength"])

    async def exists(self, path: str) -> bool:
        """``True`` for an object exactly at ``path`` or a "directory" (a prefix
        with at least one object under it). ``layout.py`` relies on the
        latter: ``db``/``@data`` are only prefixes here."""
        from botocore.exceptions import ClientError

        client = await self._get_client()
        key = _key(path)
        list_prefix = as_list_prefix(key)

        async def _probe_prefix() -> bool:
            response = await client.list_objects_v2(Bucket=self._bucket, Prefix=list_prefix, MaxKeys=1)
            return bool(response.get("Contents")) or bool(response.get("KeyCount", 0))

        with _mapped_errors(key):
            return await exists_via_prefix_probe(
                head=lambda: client.head_object(Bucket=self._bucket, Key=key),
                error_type=ClientError,
                is_not_found=lambda exc: _error_code(exc) in _NOT_FOUND_ERROR_CODES,
                probe_prefix=_probe_prefix,
            )

    async def listdir(self, path: str) -> list[Entry]:
        """Immediate entries under ``path``, each object with its ``Size``
        from the list responses; a common prefix has no size. An absent
        prefix and an empty one are indistinguishable, so both return ``[]``
        rather than raising ``NotFoundError``."""
        client = await self._get_client()
        prefix = _key(path)
        list_prefix = as_list_prefix(prefix)
        raw_entries: list[tuple[str, int | None]] = []
        paginator = client.get_paginator("list_objects_v2")
        with _mapped_errors(prefix):
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=list_prefix, Delimiter="/"):
                raw_entries.extend((common_prefix["Prefix"], None) for common_prefix in page.get("CommonPrefixes", []))
                raw_entries.extend((obj["Key"], int(obj["Size"])) for obj in page.get("Contents", []))
        return sorted_relative_entries(raw_entries, list_prefix)


async def list_buckets(**client_kwargs: Any) -> list[str]:
    """Every bucket visible to these credentials (``S3Store`` is scoped to one
    bucket). Uses a transient client, closed before returning.

    Args:
        **client_kwargs: What a caller would pass to ``S3Store``.

    Returns:
        Bucket names, sorted.

    Raises:
        PermissionDeniedError: The credentials lack account-level list
            permission.
        StorageBackendError: The request failed otherwise.
    """
    aioboto3 = _import_aioboto3()
    session = aioboto3.Session()
    with _mapped_errors(str(client_kwargs.get("endpoint_url") or "")):
        async with session.client("s3", **_with_default_timeouts(client_kwargs)) as client:
            response = await client.list_buckets()
    return sorted(bucket["Name"] for bucket in response.get("Buckets", []))
