"""``AzureStore`` — an ``ObjectStore`` implementation for Azure Blob
Storage with the same contract and lazy-import pattern as ``S3Store``;
``list_containers`` mirrors its ``list_buckets()``.

The ``aiohttp``-based client owns a session that ``AzureStore.close`` must
close. Every client gets ``_with_default_timeouts``' interactive-friendly
settings.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, override
from urllib.parse import urlsplit

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
    from azure.storage.blob.aio import BlobServiceClient

_RANGE_NOT_SATISFIABLE = 416
_NOT_FOUND = 404
_ACCESS_DENIED = frozenset({401, 403})

# Retries per request, down from azure-core's default for a batch job.
_DEFAULT_RETRY_TOTAL = 1

# No pool-size override (unlike s3.py's _DEFAULT_MAX_POOL_CONNECTIONS): aiohttp's
# default limit of 100 already exceeds S3Store's 32.


@contextmanager
def _mapped_errors(blob_name: str) -> Iterator[None]:
    """Re-raise an ``azure-core``/``aiohttp`` failure as ``ObjectStore``'s
    ``NotFoundError``/``PermissionDeniedError``/``StorageBackendError``."""
    import aiohttp
    from azure.core.exceptions import AzureError, ClientAuthenticationError, HttpResponseError

    try:
        yield
    except HttpResponseError as exc:
        if exc.status_code == _NOT_FOUND:
            raise NotFoundError("no such blob", ref=blob_name) from exc
        if exc.status_code in _ACCESS_DENIED or isinstance(exc, ClientAuthenticationError):
            raise PermissionDeniedError(f"access denied (HTTP {exc.status_code})", ref=blob_name) from exc
        raise StorageBackendError(f"Azure request failed (HTTP {exc.status_code})", ref=blob_name) from exc
    except (AzureError, aiohttp.ClientError, TimeoutError) as exc:
        raise StorageBackendError(f"Azure request failed: {exc}", ref=blob_name) from exc


def _account_name_from_url(account_url: str) -> str | None:
    """The account name from an Azure Blob endpoint URL, in either the
    ``https://<account>.blob.core.windows.net`` form or the path-style
    ``http://<host>[:<port>]/<account>`` form (Azurite, reverse proxies).
    ``None`` if neither matches."""
    parsed = urlsplit(account_url)
    host, _, suffix = parsed.netloc.partition(".blob.core.")
    if suffix and host:
        return host
    return parsed.path.strip("/") or None


def _resolve_shared_key_credential(client_kwargs: dict[str, Any]) -> dict[str, Any]:
    """``client_kwargs`` with a plain-string ``credential`` (an account key)
    turned into ``{"account_name": ..., "account_key": ...}``, deriving the
    account name from ``account_url``. The SDK's own sniffing recognizes the
    path-style form only for host ``localhost``/``127.0.0.1``, so other
    Azurite-style endpoints would raise ``ValueError``.

    Unchanged when ``credential`` isn't a string or no account name can be
    recovered."""
    credential = client_kwargs.get("credential")
    account_url = client_kwargs.get("account_url")
    if not isinstance(credential, str) or not isinstance(account_url, str):
        return client_kwargs
    account_name = _account_name_from_url(account_url)
    if account_name is None:
        return client_kwargs
    resolved = dict(client_kwargs)
    resolved["credential"] = {"account_name": account_name, "account_key": credential}
    return resolved


def _with_default_timeouts(client_kwargs: dict[str, Any]) -> dict[str, Any]:
    """``client_kwargs`` with the network timeouts and
    ``_DEFAULT_RETRY_TOTAL`` filled in; explicit caller values are kept."""
    merged = dict(client_kwargs)
    merged.setdefault("connection_timeout", NETWORK_CONNECT_TIMEOUT)
    merged.setdefault("read_timeout", NETWORK_READ_TIMEOUT)
    merged.setdefault("retry_total", _DEFAULT_RETRY_TOTAL)
    return merged


async def _force_close_transport(service_client: BlobServiceClient) -> None:
    """Force-close ``service_client``'s transport connection pool, for
    ``AzureStore.read``'s ``CancelledError`` handler (a child client's
    ``close()`` is a no-op). Reaches ``_pipeline._transport`` via ``getattr``
    and falls back to the public ``close()`` if the private chain is gone."""
    transport = getattr(getattr(service_client, "_pipeline", None), "_transport", None)
    if transport is not None:
        await transport.close()
    else:
        await service_client.close()


def _import_blob_service_client() -> type[BlobServiceClient]:
    """Lazy ``BlobServiceClient`` import; ``storage/__init__.py`` imports this
    module unconditionally."""
    from azure.storage.blob.aio import BlobServiceClient

    return BlobServiceClient


@dataclasses.dataclass(frozen=True, slots=True)
class AzureStoreDescriptor:
    """Picklable recipe for rebuilding an equivalent ``AzureStore``."""

    container: str
    client_kwargs: dict[str, Any]

    def build(self) -> AzureStore:
        return AzureStore(self.container, **self.client_kwargs)


class AzureStore(AsyncClosing):
    """An Azure Blob Storage container, addressed by ``"/"``-separated
    paths relative to the container root.

    ``client``, if given, is used as-is (tests inject a mocked
    ``BlobServiceClient``) and is not closed by ``close``; otherwise one is
    built from ``client_kwargs`` (most commonly ``account_url`` plus one of
    ``credential``/``connection_string``), with a plain-string ``credential``
    resolved by ``_resolve_shared_key_credential``.
    """

    def __init__(
        self,
        container: str,
        *,
        client: BlobServiceClient | None = None,
        **client_kwargs: Any,
    ) -> None:
        if client is not None:
            service_client = client
        else:
            BlobServiceClient = _import_blob_service_client()
            service_client = BlobServiceClient(**_with_default_timeouts(_resolve_shared_key_credential(client_kwargs)))
        self._owns_client = client is None
        # Kept verbatim so descriptor() can rebuild the store.
        self._client_kwargs = client_kwargs
        self._service_client = service_client
        self._container = service_client.get_container_client(container)

    def descriptor(self) -> AzureStoreDescriptor | None:
        """How a worker process rebuilds this store; ``None`` for one built
        on an injected ``client``, which has no picklable recipe."""
        if not self._owns_client:
            return None
        return AzureStoreDescriptor(self._container.container_name, dict(self._client_kwargs))

    @override
    def __repr__(self) -> str:
        return f"AzureStore(container={self._container.container_name!r})"

    @override
    async def close(self) -> None:
        """Close the underlying ``aiohttp`` transport.

        Only for a client this class built itself — an injected ``client``
        belongs to whoever created it. Safe to call more than once.
        """
        if self._owns_client:
            await self._service_client.close()

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        from azure.core.exceptions import HttpResponseError

        if length is not None and length <= 0:
            # download_blob has no zero-length range; the blob must still
            # exist, as for LocalFsStore.
            await self.size(path)
            return b""
        blob_name = _key(path)
        blob_client = self._container.get_blob_client(blob_name)
        with _mapped_errors(blob_name):
            try:
                downloader = await blob_client.download_blob(offset=offset, length=length)
                return await downloader.readall()
            except HttpResponseError as exc:
                if exc.status_code == _RANGE_NOT_SATISFIABLE:
                    # offset at or past the blob's size: short read at EOF.
                    return b""
                raise
            except asyncio.CancelledError:
                # No per-connection handle exists, so close the shared transport;
                # AioHttpTransport.open() rebuilds a session on the next request.
                await _force_close_transport(self._service_client)
                raise

    async def size(self, path: str) -> int:
        blob_name = _key(path)
        blob_client = self._container.get_blob_client(blob_name)
        with _mapped_errors(blob_name):
            properties = await blob_client.get_blob_properties()
        return int(properties.size)

    async def exists(self, path: str) -> bool:
        """``True`` for a blob exactly at ``path`` or a "directory" (a prefix
        with at least one blob under it). ``layout.py`` relies on the latter:
        ``db``/``@data`` are only prefixes here."""
        from azure.core.exceptions import HttpResponseError

        blob_name = _key(path)
        blob_client = self._container.get_blob_client(blob_name)
        list_prefix = as_list_prefix(blob_name)

        async def _probe_prefix() -> bool:
            async for _item in self._container.walk_blobs(name_starts_with=list_prefix, delimiter="/"):
                return True
            return False

        with _mapped_errors(blob_name):
            return await exists_via_prefix_probe(
                head=blob_client.get_blob_properties,
                error_type=HttpResponseError,
                is_not_found=lambda exc: exc.status_code == _NOT_FOUND,
                probe_prefix=_probe_prefix,
            )

    async def listdir(self, path: str) -> list[Entry]:
        """Immediate entries under ``path``, each blob with its ``size`` from
        the listing; a virtual directory prefix has no size. An absent prefix
        and an empty one are indistinguishable, so both return ``[]``, as in
        ``S3Store.listdir``."""
        prefix = _key(path)
        list_prefix = as_list_prefix(prefix)
        raw_entries: list[tuple[str, int | None]] = []
        with _mapped_errors(prefix):
            async for item in self._container.walk_blobs(name_starts_with=list_prefix, delimiter="/"):
                size = getattr(item, "size", None)
                raw_entries.append((item.name, None if size is None else int(size)))
        return sorted_relative_entries(raw_entries, list_prefix)


async def list_containers(**client_kwargs: Any) -> list[str]:
    """Every container visible to these credentials (``AzureStore`` is scoped
    to one container). Uses a transient client, closed before returning.

    Args:
        **client_kwargs: What a caller would pass to ``AzureStore``.

    Returns:
        Container names, sorted.

    Raises:
        PermissionDeniedError: The credential lacks account-level list
            permission (a container-scoped SAS token, say).
        StorageBackendError: The request failed otherwise.
    """
    BlobServiceClient = _import_blob_service_client()
    service_client = BlobServiceClient(**_with_default_timeouts(_resolve_shared_key_credential(client_kwargs)))
    try:
        with _mapped_errors(str(client_kwargs.get("account_url") or "")):
            names = [container.name async for container in service_client.list_containers()]
    finally:
        await service_client.close()
    return sorted(names)
