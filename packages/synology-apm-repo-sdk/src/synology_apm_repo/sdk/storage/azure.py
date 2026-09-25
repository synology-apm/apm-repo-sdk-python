"""``AzureStore`` — a real, executable ``ObjectStore`` implementation for
Azure Blob Storage, matching the ``s3`` module's scope, structure, and
contract exactly (same four methods, same EOF/NotFoundError semantics, same
lazy-import pattern) — this one only differs where the Azure SDK's own
shapes force a difference. ``list_containers`` mirrors that module's
``list_buckets()``.

The one genuine shape difference from ``S3Store``: Azure's hierarchical
listing (``azure.storage.blob.ContainerClient.walk_blobs``) already
returns one unified sequence of ``BlobProperties``/``BlobPrefix`` entries,
so there's no separate paginated "common prefixes" collection to merge in
the way S3's ``CommonPrefixes``/``Contents`` split requires; the SDK's own
``ItemPaged``/``AsyncItemPaged`` iterator handles continuation-token
pagination internally too.

**Why ``azure.storage.blob.aio``**: this is network I/O, and — like
``S3Store`` — this module sits on ``aiohttp``, where many outstanding
round-trips genuinely overlap on one thread; that's a real gain over a
thread-pool wrapper around a synchronous client. Its client owns an
``aiohttp`` session that must be closed, hence ``AzureStore.aclose``, which
``Session.close()`` calls.

Every client this module builds goes through ``_with_default_timeouts``,
which trims azure-core's batch-job-tuned defaults down to values an
interactive caller — the TUI's connect dialog, in particular — can
actually wait through when an endpoint is unreachable (see the
module-level constants below for the exact values).
"""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlsplit

from ..errors import NotFoundError
from .base import backend_key as _key
from .prefix_listing import as_list_prefix, exists_via_prefix_probe, sorted_relative_names

_RANGE_NOT_SATISFIABLE = 416
_NOT_FOUND = 404

# azure-core's own defaults (300s connect, 300s read, plus a handful of
# retries) are tuned for long-running batch jobs, not an interactive "is
# this endpoint even reachable" probe. These apply to every client this
# module builds unless the caller already passed the same keyword
# explicitly, in which case the caller's value wins.
_DEFAULT_CONNECTION_TIMEOUT = 5
_DEFAULT_READ_TIMEOUT = 15
_DEFAULT_RETRY_TOTAL = 1

# Unlike storage/s3.py's own _DEFAULT_MAX_POOL_CONNECTIONS override,
# nothing needs overriding here:
# azure.core.pipeline.transport._aiohttp.AioHttpTransport.open() builds a
# plain aiohttp.ClientSession() with no connector= override at all, so it
# inherits aiohttp.TCPConnector's own default of limit=100 (global) /
# limit_per_host=0 (unlimited, bounded only by that 100) -- already well
# above _DEFAULT_MAX_POOL_CONNECTIONS's 32. Every sub-client this module
# hands out (via get_blob_client()) shares that one parent
# transport/session rather than opening its own, which is why read()'s
# CancelledError handler has to force-close that one shared transport on
# cancellation (no per-connection handle exists to close instead) rather
# than just the connection the cancelled read was using.


def _account_name_from_url(account_url: str) -> str | None:
    """Best-effort account name extraction from an Azure Blob endpoint URL —
    covers both the production ``https://<account>.blob.core.windows.net``
    subdomain form and the path-style form a custom endpoint (Azurite, a
    reverse proxy, ...) uses instead: ``http://<host>[:<port>]/<account>``.
    Returns ``None`` when neither pattern matches — a bare host with no path
    and no recognizable subdomain, genuinely ambiguous."""
    parsed = urlsplit(account_url)
    host, _, suffix = parsed.netloc.partition(".blob.core.")
    if suffix and host:
        return host
    return parsed.path.strip("/") or None


def _resolve_shared_key_credential(client_kwargs: dict[str, Any]) -> dict[str, Any]:
    """``client_kwargs`` with a plain-string ``credential`` (an account
    key) turned into the explicit ``{"account_name": ..., "account_key":
    ...}`` form ``BlobServiceClient`` falls back to internally, but with
    the account name derived from ``account_url`` here rather than left
    to the SDK's own sniffing: that logic only recognizes the path-style
    form when the host is literally ``"localhost"``/``"127.0.0.1"``, so
    any other Azurite-style endpoint (a real hostname, a remote IP, a
    docker service name) raises ``ValueError`` before ever making a
    request, even though the account name is right there in the URL's
    path.

    Left untouched when ``credential`` isn't a plain string, or no
    account name can be recovered from ``account_url`` — the SDK's own
    fallback still applies then."""
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
    """``client_kwargs`` with interactive-friendly connect/read timeouts and
    a low retry cap filled in (values and rationale: the module-level
    constants above). Uses ``setdefault`` rather than always overwriting,
    so a caller who already passed
    ``connection_timeout``/``read_timeout``/``retry_total`` explicitly keeps
    their own value."""
    merged = dict(client_kwargs)
    merged.setdefault("connection_timeout", _DEFAULT_CONNECTION_TIMEOUT)
    merged.setdefault("read_timeout", _DEFAULT_READ_TIMEOUT)
    merged.setdefault("retry_total", _DEFAULT_RETRY_TOTAL)
    return merged


async def _force_close_transport(service_client: Any) -> None:
    """Force-close ``service_client``'s underlying transport connection
    pool — what ``AzureStore.read``'s own ``CancelledError`` handler
    needs (see its docstring for why the public ``close()``/a child
    client's own no-op ``AsyncTransportWrapper.close()`` aren't enough).
    Walks ``_pipeline``/``_transport`` via ``getattr(..., None)`` rather
    than assuming the chain exists, so a future ``azure-storage-blob``
    upgrade that renames/removes either private attribute degrades to
    the public ``close()`` instead of raising ``AttributeError`` at a
    cancellation-critical moment."""
    transport = getattr(getattr(service_client, "_pipeline", None), "_transport", None)
    if transport is not None:
        await transport.close()
    else:
        await service_client.close()


def _import_blob_service_client() -> Any:
    """Lazy ``from azure.storage.blob.aio import BlobServiceClient``:
    ``azure-storage-blob`` is always installed (a required dependency) but
    pulls in a substantial import graph, and ``storage/__init__.py`` imports
    this module unconditionally, so a module-level import would make every
    caller of ``storage`` pay that cost even if it never touches
    ``AzureStore``. Shared by every constructor/free function here that
    needs it, so the choice of what to import lives in one place."""
    from azure.storage.blob.aio import BlobServiceClient

    return BlobServiceClient


class AzureStore:
    """An Azure Blob Storage container, addressed by ``"/"``-separated
    paths relative to the container root.

    ``client``, if given, is used as-is (tests inject a mocked
    ``BlobServiceClient`` this way — the contract this class must get
    right is status-code-to-``ObjectStore``-semantics translation, not
    anything a live Azurite instance would exercise differently);
    otherwise one is built from ``client_kwargs`` (most commonly
    ``account_url``, one of ``credential``/``connection_string``) after
    ``_resolve_shared_key_credential`` resolves a plain-string
    ``credential`` against ``account_url``.
    """

    def __init__(
        self,
        container: str,
        *,
        client: Any = None,
        **client_kwargs: Any,
    ) -> None:
        if client is not None:
            service_client = client
        else:
            BlobServiceClient = _import_blob_service_client()
            service_client = BlobServiceClient(**_with_default_timeouts(_resolve_shared_key_credential(client_kwargs)))
        self._owns_client = client is None
        # Kept verbatim (pre-timeout/credential-resolution), unused by this
        # class itself, purely so storage.store_descriptor.describe_store()
        # can recover the exact constructor arguments that would rebuild an
        # equivalent store elsewhere — the same reason S3Store already
        # keeps its own ``_client_kwargs``.
        self._client_kwargs = client_kwargs
        self._service_client = service_client
        self._container = service_client.get_container_client(container)

    def __repr__(self) -> str:
        return f"AzureStore(container={self._container.container_name!r})"

    async def aclose(self) -> None:
        """Close the underlying ``aiohttp`` transport.

        Only for a client this class built itself — an injected ``client``
        belongs to whoever created it. Safe to call more than once.
        """
        if self._owns_client:
            await self._service_client.close()

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        from azure.core.exceptions import HttpResponseError

        blob_name = _key(path)
        blob_client = self._container.get_blob_client(blob_name)
        try:
            downloader = await blob_client.download_blob(offset=offset, length=length)
            return await downloader.readall()  # type: ignore[no-any-return]
        except HttpResponseError as exc:
            if exc.status_code == _RANGE_NOT_SATISFIABLE:
                # offset is at or past the blob's real size - the same
                # "short read at EOF" every other ObjectStore backend
                # already returns as b"" rather than raising.
                return b""
            if exc.status_code == _NOT_FOUND:
                raise NotFoundError("no such blob", ref=blob_name) from exc
            raise
        except asyncio.CancelledError:
            # Uses the same approach as S3Store.read() for this identical
            # hazard: Azure's async SDK gives no per-connection handle to
            # close on cancellation, so a read large enough to split into
            # multiple ranged GETs can leave sibling asyncio Tasks running
            # against this store's connection pool after this coroutine is
            # cancelled — not reachable via ``downloader``/``blob_client``
            # at all. The only reliably-reachable handle is the top-level
            # client's transport (``self._service_client``); closing it is
            # blunt (every connection in the pool goes, not just this
            # read's), but ``AioHttpTransport.open()`` lazily rebuilds a
            # fresh session on the next request, so the store stays usable
            # immediately afterward.
            await _force_close_transport(self._service_client)
            raise

    async def size(self, path: str) -> int:
        from azure.core.exceptions import HttpResponseError

        blob_name = _key(path)
        blob_client = self._container.get_blob_client(blob_name)
        try:
            properties = await blob_client.get_blob_properties()
        except HttpResponseError as exc:
            if exc.status_code == _NOT_FOUND:
                raise NotFoundError("no such blob", ref=blob_name) from exc
            raise
        return int(properties.size)

    async def exists(self, path: str) -> bool:
        """``True`` for either a blob exactly at ``path``, or a
        "directory" — a prefix with at least one blob under it: ``db``/
        ``@data`` are never blobs in their own right on an object-storage
        layout, only prefixes with real blobs underneath (``layout.py``
        relies on this)."""
        from azure.core.exceptions import HttpResponseError

        blob_name = _key(path)
        blob_client = self._container.get_blob_client(blob_name)
        list_prefix = as_list_prefix(blob_name)

        async def _probe_prefix() -> bool:
            async for _item in self._container.walk_blobs(name_starts_with=list_prefix, delimiter="/"):
                return True
            return False

        return await exists_via_prefix_probe(
            head=blob_client.get_blob_properties,
            error_type=HttpResponseError,
            is_not_found=lambda exc: exc.status_code == _NOT_FOUND,
            probe_prefix=_probe_prefix,
        )

    async def listdir(self, path: str) -> list[str]:
        """An absent "directory" (a prefix with zero blobs under it) and an
        empty one are indistinguishable in an object store with no real
        directory entities — both correctly report ``[]``, the same as
        ``S3Store.listdir``."""
        prefix = _key(path)
        list_prefix = as_list_prefix(prefix)
        raw_names = [
            item.name async for item in self._container.walk_blobs(name_starts_with=list_prefix, delimiter="/")
        ]
        return sorted_relative_names(raw_names, list_prefix)


async def list_containers(**client_kwargs: Any) -> list[str]:
    """Every container visible to these credentials — a container-*less*
    operation ``AzureStore`` has no method for, since all four of its
    methods are scoped to one chosen container. Builds its own transient
    client the same lazy-import way ``AzureStore`` does and closes it
    before returning; no lifecycle for a caller to manage beyond this one
    call.

    ``client_kwargs`` is exactly what a caller would otherwise pass to
    ``AzureStore``. Raises whatever the underlying SDK call raises —
    most notably ``HttpResponseError`` when the credential has no
    account-level "list containers" permission, which a container-scoped
    SAS token never does — unhandled, the same as every other
    backend-specific exception ``AzureStore``'s own methods let
    propagate.
    """
    BlobServiceClient = _import_blob_service_client()
    service_client = BlobServiceClient(**_with_default_timeouts(_resolve_shared_key_credential(client_kwargs)))
    try:
        names = [container.name async for container in service_client.list_containers()]
    finally:
        await service_client.close()
    return sorted(names)
