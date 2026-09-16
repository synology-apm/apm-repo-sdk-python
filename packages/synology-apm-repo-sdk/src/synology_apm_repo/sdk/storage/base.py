"""The ``ObjectStore`` protocol — the sole boundary between "how bytes are
fetched" and everything above it.

Deliberately four methods, all pure byte/existence semantics. Nothing here
knows about sequence-id suffixes, SQLite, WAL files, or repository layout —
those are built *on top of* an ``ObjectStore``, in this same package
(``seqid``, ``layout``, ``sqlite``, ``generations``), not inside it. This keeps the cost of adding a new backend
(S3, Azure) to "implement four methods", never "also learn how SQLite WAL
recovery or repository layout works".

All paths are ``"/"``-separated strings *relative to the store's root*, never
``pathlib.Path`` and never absolute — a backend like S3 has no notion of "the
current working directory" or OS path separators, and requiring callers to
speak in relative strings keeps that assumption from leaking upward.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ObjectStore(Protocol):
    """Read-only, byte-oriented access to one repository's storage tree.

    All four methods are ``async def``, but what that buys differs
    sharply per backend:

    - ``LocalFsStore`` has no genuinely non-blocking option —
      ``os.pread``/``os.open``/etc. have no native async form in CPython,
      so its four methods are thin ``asyncio.to_thread()`` wrappers around
      synchronous syscalls, the same as ``aiofiles`` does internally.
    - ``S3Store`` and ``AzureStore`` are the backends where async is
      *real*: both sit on ``aiohttp``, so many outstanding network
      round-trips genuinely overlap on one thread.

    Implementations MUST be safe to call concurrently, whether that
    concurrency is several asyncio Tasks on one event loop or several
    real OS threads out of ``asyncio.to_thread()``'s executor.

    Implementations MUST NOT expose any mutating method —
    ``@runtime_checkable`` only verifies the four methods below exist, not
    that nothing else does; the read-only guarantee rests on each
    implementer's own discipline.
    """

    async def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        """Return up to ``length`` bytes starting at ``offset`` in ``path``.

        ``length=None`` reads to end-of-file. Raises ``NotFoundError`` if
        ``path`` does not exist, or ``PermissionDeniedError`` if it exists but
        access was denied. Short reads at end-of-file return fewer bytes
        than requested, never pad or raise.
        """
        ...

    async def size(self, path: str) -> int:
        """Return the total byte length of ``path``.

        Raises ``NotFoundError`` if it does not exist, or ``PermissionDeniedError`` if
        it exists but access was denied.
        """
        ...

    async def exists(self, path: str) -> bool:
        """Return whether ``path`` exists (file or directory). Never raises
        for a merely-absent path — that is exactly what ``False`` means.
        Also returns ``False`` (rather than raising ``PermissionDeniedError``)
        when access is denied: callers use this method to probe candidate
        paths, often several irrelevant ones per real hit, and a probe that
        can raise would abort that search instead of just ruling one
        candidate out."""
        ...

    async def listdir(self, path: str) -> list[str]:
        """Return the immediate entry names (files and subdirectories, not
        full paths) directly under ``path``, in unspecified order.

        Raises ``NotFoundError`` if ``path`` does not exist or is not a
        directory, or ``PermissionDeniedError`` if it exists but access was
        denied.
        """
        ...


@runtime_checkable
class AsyncCloseable(Protocol):
    """An ``ObjectStore`` that owns a real client/connector needing release —
    ``S3Store``/``AzureStore``'s ``aiohttp`` connector, not ``LocalFsStore``,
    which has nothing to close. Lets callers holding a bare ``ObjectStore``
    (``Session.close()``) release it with ``isinstance(store, AsyncCloseable)``
    instead of duck-typing ``getattr(store, "aclose", None)``."""

    async def aclose(self) -> None: ...


async def aclose_if_possible(store: ObjectStore) -> None:
    """``await store.aclose()`` when ``store`` is ``AsyncCloseable``, a
    no-op otherwise — the guarded-close check every caller holding a bare
    ``ObjectStore`` it must release on its own repeats (``Session.close()``,
    ``recording.py``'s ``_InstrumentedStore.aclose()``, the CLI's ``dump``
    command family), lives here once next to the protocol it checks."""
    if isinstance(store, AsyncCloseable):
        await store.aclose()


def join_path(*parts: str) -> str:
    """Join ``parts`` into one store-relative path, ``"/"``-separated,
    tolerating an empty/absent segment (``repo_root`` is often ``""``)
    and any stray leading/trailing ``"/"`` a caller's own segment
    happens to carry. Callers across every layer above this one build
    every ``ObjectStore`` path this way — lives here, next to the
    ``ObjectStore`` protocol whose own path convention it implements."""
    return "/".join(p.strip("/") for p in parts if p)


def backend_key(path: str) -> str:
    """Strip ``path``'s leading/trailing ``"/"`` for a backend whose own
    key/blob-name namespace has none — S3's object keys and Azure's blob
    names both reject a leading ``"/"``, unlike this SDK's own
    ``ObjectStore`` path convention (which tolerates one, per
    ``join_path``). Shared by ``S3Store`` and ``AzureStore`` rather than
    each defining an identical private helper."""
    return path.strip("/")
