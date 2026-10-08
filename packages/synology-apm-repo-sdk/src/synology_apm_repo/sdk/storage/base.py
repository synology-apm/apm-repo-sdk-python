"""The ``ObjectStore`` protocol — the sole boundary between "how bytes are
fetched" and everything above it.

Four read methods of pure byte/existence semantics, plus ``close()``. Sequence-id suffixes, SQLite,
WAL files and repository layout are built on top of an ``ObjectStore`` in
this package (``seqid``, ``layout``, ``sqlite``, ``generations``), so a new
backend only implements those five.

All paths are ``"/"``-separated strings relative to the store's root, never
``pathlib.Path`` and never absolute; a backend like S3 has no working
directory or OS path separator.
"""

from __future__ import annotations

from typing import NamedTuple, Protocol, runtime_checkable

from ..errors import NotFoundError

NETWORK_CONNECT_TIMEOUT = 5
"""Seconds a network store waits to connect. The client libraries'
batch-tuned defaults are capped to what an interactive caller, such as
the TUI's connect dialog, can wait through."""

NETWORK_READ_TIMEOUT = 15
"""Seconds a network store waits for one request's response; see
``NETWORK_CONNECT_TIMEOUT``."""


class Entry(NamedTuple):
    """One entry ``ObjectStore.listdir`` reports."""

    name: str
    """The entry's own name, not a path."""
    size: int | None
    """The file's size in bytes, from the listing itself (every built-in
    backend's listing carries it); ``None`` for a directory, or a size the
    backend did not report."""


@runtime_checkable
class ObjectStore(Protocol):
    """Read-only, byte-oriented access to one repository's storage tree.

    Every method is ``async def``: ``LocalFsStore`` and ``SmbStore`` run
    their blocking calls on worker threads, while ``S3Store`` and
    ``AzureStore`` sit on ``aiohttp``.

    Implementations MUST be safe to call concurrently, from several asyncio
    Tasks or from OS threads.

    Implementations MUST NOT expose any mutating method;
    ``@runtime_checkable`` checks only that the methods below exist.

    Every failure surfaces as an ``ApmRepoError``: ``NotFoundError``,
    ``PermissionDeniedError``, or ``StorageBackendError`` for anything else
    (network, timeout, service or OS I/O error) — never a client library's
    own exception.
    """

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        """Return up to ``length`` bytes starting at ``offset`` in ``path``.

        ``length=None`` reads to end-of-file. Short reads at end-of-file return
        fewer bytes than requested, never pad or raise.

        Raises:
            NotFoundError: ``path`` does not exist.
            PermissionDeniedError: Access was denied.
            StorageBackendError: The backend failed to serve the request.
        """
        ...

    async def size(self, path: str) -> int:
        """Return the total byte length of ``path``.

        Raises:
            NotFoundError: ``path`` does not exist.
            PermissionDeniedError: Access was denied.
            StorageBackendError: The backend failed to serve the request.
        """
        ...

    async def exists(self, path: str) -> bool:
        """Return whether ``path`` exists (file or directory). Never raises
        for a merely-absent path. ``LocalFsStore`` and ``SmbStore`` also
        return ``False`` when access is denied, so probing candidate paths
        never aborts a search; ``S3Store``/``AzureStore`` raise
        ``PermissionDeniedError`` for it. Any backend raises
        ``StorageBackendError`` when the request itself fails."""
        ...

    async def listdir(self, path: str) -> list[Entry]:
        """Return the immediate entries (files and subdirectories) directly
        under ``path``, sorted by name, each with the size its listing
        reported, so a caller needing sizes stats nothing.

        Raises:
            NotFoundError: ``path`` does not exist or is not a directory
                (``LocalFsStore``, ``SmbStore``). ``S3Store``/``AzureStore``
                have no directories and return ``[]`` for an absent prefix.
            PermissionDeniedError: Access was denied.
            StorageBackendError: The backend failed to serve the request.
        """
        ...

    async def close(self) -> None:
        """Release what the store holds (network clients, sessions, threads);
        a no-op for one holding nothing, such as ``LocalFsStore``. Safe to
        call more than once."""
        ...


async def list_names(store: ObjectStore, path: str) -> list[str]:
    """``store.listdir(path)``'s names alone, sorted."""
    return [entry.name for entry in await store.listdir(path)]


@runtime_checkable
class SyncReadable(Protocol):
    """An ``ObjectStore`` whose ``read`` is a blocking call it hops to a
    thread anyway (``LocalFsStore``), offering that call directly, so a
    caller already on a worker thread reads without a second hop. Optional:
    a wrapper such as ``TracingStore`` does not offer it, so its reads keep
    going through the wrapper."""

    def read_sync(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        """``ObjectStore.read``'s contract, blocking the calling thread."""
        ...


def join_path(*parts: str) -> str:
    """Join ``parts`` into one store-relative, ``"/"``-separated path,
    skipping empty parts (``repo_root`` is often ``""``) and stripping stray
    leading/trailing ``"/"``. Every ``ObjectStore`` path is built this way.

    Every ``"/"``-separated segment of every part is checked, since one part
    may be a catalog-derived string containing several segments. Rejected:

    - a ``".."`` segment, which would escape the store root.
    - a segment containing a backslash: the convention is ``"/"`` only, and on
      Windows a backslash would later be read as a separator by ``pathlib``.

    This is the primary path-safety check (``SmbStore`` has none of its own);
    ``LocalFsStore`` also checks the assembled path as a backstop.

    Raises:
        NotFoundError: a segment is ``".."`` or contains a backslash.
    """
    joined = "/".join(p.strip("/") for p in parts if p)
    for segment in joined.split("/"):
        if segment == "..":
            raise NotFoundError(f"path segment {segment!r} escapes the store root", ref=joined)
        if "\\" in segment:
            raise NotFoundError(f"path segment {segment!r} contains a backslash", ref=joined)
    return joined


def backend_key(path: str) -> str:
    """Strip ``path``'s leading/trailing ``"/"`` for S3 object keys and Azure
    blob names, which reject a leading ``"/"``."""
    return path.strip("/")
