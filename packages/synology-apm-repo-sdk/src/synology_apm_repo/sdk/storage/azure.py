"""``AzureStore`` — an ``ObjectStore`` implementation for Azure Blob
Storage, matching ``S3Store``'s scope, structure, and contract (same four
methods, same EOF/``NotFoundError`` semantics, same lazy-import pattern).
``list_containers`` mirrors that module's ``list_buckets()``.

Unlike S3, Azure's hierarchical listing
(``azure.storage.blob.ContainerClient.walk_blobs``) already returns one
unified sequence of entries, so there's no separate "common prefixes"
collection to merge in.

This module sits on ``aiohttp`` (via ``azure.storage.blob.aio``), the same
as ``S3Store`` — its client owns a session that must be closed, hence
``AzureStore.aclose``. Every client built here goes through
``_with_default_timeouts``, which trims azure-core's batch-job-tuned
defaults to values an interactive caller can wait through (see the
module-level constants below).
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
# retries) are tuned for a batch job, not an interactive reachability probe.
# These apply unless the caller already passed the same keyword explicitly.
_DEFAULT_CONNECTION_TIMEOUT = 5
_DEFAULT_READ_TIMEOUT = 15
_DEFAULT_RETRY_TOTAL = 1

# No pool-size override needed here (unlike storage/s3.py's
# _DEFAULT_MAX_POOL_CONNECTIONS): azure-core's transport builds a plain
# aiohttp.ClientSession() with no connector override, inheriting aiohttp's
# own default limit=100 — already above S3Store's 32. Every sub-client this
# module hands out shares that one parent transport/session, which is why
# read()'s CancelledError handler force-closes the whole transport rather
# than just its own connection (see that handler).


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
    """Lazy ``from azure.storage.blob.aio import BlobServiceClient`` —
    ``storage/__init__.py`` imports this module unconditionally, so a
    module-level import would make every caller pay its import cost even
    when it never touches ``AzureStore``."""
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
            # Azure's async SDK gives no per-connection handle to close on
            # cancellation, only the shared parent transport (see the
            # module-level pool-size comment) — blunt, but
            # AioHttpTransport.open() lazily rebuilds a fresh session on the
            # next request, so the store stays usable immediately after.
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
