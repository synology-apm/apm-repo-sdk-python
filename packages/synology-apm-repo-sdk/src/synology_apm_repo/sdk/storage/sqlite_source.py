"""``peel()`` + ``SqliteSource`` — unify the six envelope->SQLite paths
this project has:

======================================  ==========
source                                  envelope chain
======================================  ==========
repository ``db/<name>``                raw
``copy_meta_file/*/target.db``          aHlT? -> raw
``version.db.zst``                      aHlT? -> zstd
``saas/*/db/saas_{version,snapshot}``   raw
embedded ``ObjectDB`` (saas_obj slice)  raw
service-level DB (saas_obj slice)       zstd
======================================  ==========

Only two envelope kinds exist (``aHlT`` AES-CTR, then optionally a
standard ZSTD frame) and each is auto-detected by its own magic bytes, so
one small function handles every source in the table above — callers
never need to know in advance which of the six they have.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import os
import tempfile
from collections.abc import Callable
from types import TracebackType
from typing import Self

import aiosqlite

from ..errors import KeyRequiredError
from ..format.compression import ZSTD_FRAME_MAGIC, decompress_zstd_stream
from ..format.crypto import ahlt_decrypt
from ..format.headers import MAGIC
from .base import ObjectStore
from .sqlite import open_sqlite

_AHLT_MAGIC = MAGIC["ahlt"]


def _write_fd(fd: int, data: bytes) -> None:
    with os.fdopen(fd, "wb") as f:
        f.write(data)


def is_zstd_frame(head: bytes) -> bool:
    """Whether ``head`` opens with a standard ZSTD frame's magic number
    — only the first 4 bytes are ever inspected, so ``head`` need not be
    the full payload. The exact check ``peel`` uses internally to
    decide whether to attempt decompression at all; exposed here so a
    caller that only wants to know *whether* something is zstd-framed
    before deciding how much to even read (e.g.
    ``units/saas/services.py``'s ``inspect_object``, choosing between a
    small head read and a full one) doesn't need its own copy of the Codec
    Layer's magic constant — see ``ARCHITECTURE.md``'s "Cross-cutting
    shared mechanisms" on why drifting from ``peel()``'s own convention
    is treated as a real bug, not a style nit."""
    return head[:4] == ZSTD_FRAME_MAGIC


class Envelope(enum.Enum):
    """Which wrapper(s) ``peel`` stripped, outermost first — purely
    informational (diagnostics/``--verbose``), callers never need to
    branch on it themselves."""

    AHLT = "aHlT"
    ZSTD = "zstd"


def peel(
    data: bytes, *, vault_key: bytes | None = None, max_zstd_output_size: int | None = None
) -> tuple[bytes, list[Envelope]]:
    """Strip whichever envelope(s) ``data`` actually has, auto-detected by
    magic, returning ``(payload, envelopes_stripped)``.

    Raises ``KeyRequiredError`` if the data is ``aHlT``-enveloped but no
    ``vault_key`` was given. Never raises for the zstd step failing to
    apply — a payload simply isn't zstd-framed if its first 4 bytes don't
    match, a perfectly valid outcome (e.g. a plain ``db/<name>`` file).

    ``max_zstd_output_size``, forwarded to ``decompress_zstd_stream``,
    bounds how much a genuinely zstd-matching frame may decompress to
    before raising — for callers peeling content of an *unconfirmed*
    type; omit it (default) for a source already known to be a trusted
    db snapshot.
    """
    envelopes: list[Envelope] = []
    if data[:4] == _AHLT_MAGIC:
        if vault_key is None:
            raise KeyRequiredError("data is aHlT-enveloped but no vault_key was given")
        data = ahlt_decrypt(data, vault_key)
        envelopes.append(Envelope.AHLT)
    if is_zstd_frame(data):
        data = decompress_zstd_stream(data, max_output_size=max_zstd_output_size)
        envelopes.append(Envelope.ZSTD)
    return data, envelopes


class SqliteSource:
    """Materialize raw SQLite bytes to a private temp file and open a
    connection to it — the common tail end of all six paths in this
    module's docstring, once ``peel`` has produced plain SQLite bytes.

    Constructed via ``await SqliteSource.from_bytes(...)`` /
    ``await SqliteSource.from_raw_store(...)``, async classmethod
    factories — see ``Table.create``.

    Async-context-manager friendly (``async with``); also safe to just
    ``await`` ``close`` directly once done. The temp file is created
    with a random name in the platform temp dir and unlinked on close —
    never touches the source repository (read-only invariant). The
    connection onto that temp file is deliberately writable; see
    ``from_bytes``.
    """

    def __init__(self) -> None:
        self._path: str | None = None
        self._tmp_dir: tempfile.TemporaryDirectory[str] | None = None
        self.connection: aiosqlite.Connection
        self._closed = False

    @classmethod
    async def from_bytes(cls, data: bytes) -> Self:
        """Materialize plain (post-``peel``) SQLite ``data`` to a private
        temp file and open a *writable* connection to it.

        Writable because the file is this instance's own scratch copy,
        unlinked again by ``close()`` — the store's bytes are already behind
        us by the time ``data`` exists, so the read-only invariant is upheld
        by what this never opens, not by the mode of this connection. It is
        also what lets ``apply_index_hint`` build a real index here instead
        of leaving every hinted query a full table scan."""
        fd, path = tempfile.mkstemp(suffix=".db")
        try:
            await asyncio.to_thread(_write_fd, fd, data)
            self = cls()
            self._path = path
            # ``rw``, not the default ``rwc`` -- same reasoning as
            # ``sqlite.py``'s own private WAL-recovery copy: a path SQLite
            # cannot open here is a real failure too, not a signal to
            # silently create a new, empty database.
            self.connection = await aiosqlite.connect(f"file:{path}?mode=rw", uri=True)
        except Exception:
            os.unlink(path)
            raise
        return self

    @classmethod
    async def from_raw_store(cls, store: ObjectStore, path: str) -> Self:
        """Read and open ``path`` from ``store``, for a caller that
        already knows ``path`` is never enveloped (``db/<name>``,
        ``saas/*/db/saas_{version,snapshot}``). Delegates to
        ``open_sqlite``, which materializes ``path`` itself and also
        handles a real non-empty ``-wal`` sidecar.
        """
        return await cls._from_open_sqlite(store, path)

    @classmethod
    async def from_enveloped_store(cls, store: ObjectStore, path: str, *, vault_key: bytes | None) -> Self:
        """Read, ``peel()``, and open ``path`` from ``store`` — for a source
        that may be ``aHlT``-enveloped (``copy_meta_file/*/target.db``,
        FORMAT-SPEC.md §6.1) and may have a real ``-wal``/``-shm`` sidecar,
        each peeled independently."""

        def _peel(raw: bytes) -> bytes:
            payload, _envelopes = peel(raw, vault_key=vault_key)
            return payload

        return await cls._from_open_sqlite(store, path, transform=_peel)

    @classmethod
    async def _from_open_sqlite(
        cls, store: ObjectStore, path: str, *, transform: Callable[[bytes], bytes] | None = None
    ) -> Self:
        source = cls()
        conn, tmp = await open_sqlite(store, path, transform=transform)
        source.connection = conn
        # ``source`` is a second instance of this same class, so setting its
        # private attribute here is the class initializing its own
        # instance's state, not reaching into an unrelated object's
        # internals.
        source._tmp_dir = tmp
        return source

    async def close(self) -> None:
        """Safe to call more than once — a second call is a no-op rather
        than raising ``FileNotFoundError`` on the already-unlinked temp
        file. A caller that closes a provider itself, ahead of the
        session-wide cleanup that would otherwise close it again at
        session end, must not crash that later, redundant close."""
        if self._closed:
            return
        self._closed = True
        await self.connection.close()
        if self._path is not None:
            os.unlink(self._path)
            # A read-write connection can leave sidecars next to the temp
            # file; best-effort because most closes never produce any.
            for suffix in ("-wal", "-shm", "-journal"):
                with contextlib.suppress(OSError):
                    os.unlink(self._path + suffix)
        if self._tmp_dir is not None:
            self._tmp_dir.cleanup()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
