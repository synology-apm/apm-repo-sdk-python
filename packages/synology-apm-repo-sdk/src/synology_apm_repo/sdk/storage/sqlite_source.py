"""``peel()`` and ``SqliteSource``: one way to open each of the six
envelope -> SQLite sources:

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
standard ZSTD frame), each detected by its magic bytes, so callers needn't
know which source they have.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import os
import tempfile
from collections.abc import AsyncIterator, Callable, Iterable, Iterator
from pathlib import Path
from typing import BinaryIO, Self, override

import aiosqlite
import zstandard

from .._util.closing import AsyncClosing, shield_or_undo
from ..errors import DataCorruptError, KeyRequiredError
from ..format.compression import (
    decompress_zstd_stream,
    is_zstd_frame,
    iter_decompressed_zstd,
    zstd_content_size,
)
from ..format.crypto import ahlt_decrypt, is_ahlt
from .base import ObjectStore
from .disk_space import DiskReservation, reserve_disk_space
from .sqlite import connect, open_sqlite

#: Fallback ceiling on ``target.db``'s decompressed size, used only when a
#: zstd frame declares none (a declared size is enforced instead — see
#: ``format.compression.iter_decompressed_zstd``). A real ``target.db`` is
#: never zstd-framed, so this only bounds a hostile payload faking the magic.
_MAX_TARGET_DB_DECOMPRESS_SIZE_FALLBACK = 256 << 20  # 256 MiB


def _effective_zstd_output_size(plain: bytes | bytearray, fallback: int | None) -> int | None:
    """The most bytes writing ``plain`` (already past any ``aHlT`` step)
    can produce: the zstd frame's declared content size, ``len(plain)`` if
    it isn't zstd-framed, else ``fallback``."""
    if not is_zstd_frame(plain):
        return len(plain)
    declared = zstd_content_size(plain)
    return declared if declared is not None else fallback


def _within_budget(pieces: Iterable[bytes], reservation: DiskReservation) -> Iterator[bytes]:
    """``pieces``, each counted against ``reservation`` before it is
    written (see ``DiskReservation.account``)."""
    for piece in pieces:
        reservation.account(len(piece))
        yield piece


class Envelope(enum.Enum):
    """Which wrapper(s) ``peel`` stripped, outermost first; informational
    (diagnostics/``--verbose``)."""

    AHLT = "aHlT"
    ZSTD = "zstd"


def _ahlt_decrypt_if_enveloped(
    data: bytes | bytearray, *, vault_key: bytes | None
) -> tuple[bytes | bytearray, list[Envelope]]:
    """Strips ``data``'s ``aHlT`` wrapper if present, else returns it
    unchanged; the (possibly still zstd-framed) result is what the zstd step
    takes.

    Raises:
        KeyRequiredError: ``data`` is ``aHlT``-enveloped but no
            ``vault_key`` was given.
    """
    if is_ahlt(data):
        if vault_key is None:
            raise KeyRequiredError("data is aHlT-enveloped but no vault_key was given")
        return ahlt_decrypt(data, vault_key), [Envelope.AHLT]
    return data, []


def _peel_into(
    data: bytes | bytearray, dest: BinaryIO, *, vault_key: bytes | None, max_output_size: int | None, dir_path: Path
) -> list[Envelope]:
    """``peel()``, streaming into ``dest`` (a file under ``dir_path``) so a
    large decompressed payload is never one in-memory ``bytes``: aHlT-decrypt
    in memory, reserve room under ``dir_path`` (see ``disk_space``), then
    write the zstd step's pieces. Returns the envelopes stripped."""
    plain, envelopes = _ahlt_decrypt_if_enveloped(data, vault_key=vault_key)
    with reserve_disk_space(dir_path, _effective_zstd_output_size(plain, max_output_size)) as reservation:
        if is_zstd_frame(plain):
            dest.writelines(_within_budget(iter_decompressed_zstd(plain, max_output_size=max_output_size), reservation))
            return [*envelopes, Envelope.ZSTD]
        dest.write(plain)
    return envelopes


def _write_reserved(data: bytes | bytearray, dest: BinaryIO, dir_path: Path) -> None:
    """Write ``data`` to ``dest`` (a file under ``dir_path``) once
    ``reserve_disk_space`` lets it."""
    with reserve_disk_space(dir_path, len(data)):
        dest.write(data)


async def _write_temp_file[T](write: Callable[[BinaryIO], T], *, dir_path: Path) -> tuple[str, T]:
    """Run ``write`` on a new ``mkstemp()`` file under ``dir_path`` in a
    worker thread; returns the file's path and ``write``'s result. The file
    is unlinked on any failure, cancellation included, once the thread has
    closed it (Windows can't unlink an open file)."""
    fd, path = tempfile.mkstemp(suffix=".db", dir=dir_path)

    def run() -> T:
        with os.fdopen(fd, "wb") as f:
            return write(f)

    async def unlink(_: object) -> None:
        os.unlink(path)

    return path, await shield_or_undo(asyncio.to_thread(run), unlink)


async def _connect_temp_file(path: str) -> aiosqlite.Connection:
    """A writable connection to the private temp copy at ``path`` (``rw``,
    not ``rwc``: never silently create an empty database); unlinks it if
    the connect fails or is cancelled."""
    try:
        return await connect(f"file:{path}?mode=rw")
    except BaseException:
        os.unlink(path)
        raise


def peel(
    data: bytes | bytearray, *, vault_key: bytes | None = None, max_zstd_output_size: int | None
) -> tuple[bytes | bytearray, list[Envelope]]:
    """Strip whichever envelope(s) ``data`` actually has, auto-detected by
    magic, returning ``(payload, envelopes_stripped)``. A payload that isn't
    zstd-framed is a valid outcome, not an error.

    ``max_zstd_output_size`` is mandatory and only the fallback ceiling for
    a zstd frame that declares no size (see
    ``format.compression.iter_decompressed_zstd``); pass ``None`` explicitly
    only for a source already known to be trusted.

    Raises:
        KeyRequiredError: ``data`` is ``aHlT``-enveloped but no
            ``vault_key`` was given.
        DataCorruptError: The ``aHlT`` header fails its magic or CRC check,
            or a zstd frame's declared content size doesn't match what
            decompression produced.
        FormatError: ``data`` has the ``aHlT`` magic but is shorter than
            its 64-byte header.
        zstandard.ZstdError: The zstd frame is malformed or exceeds its
            ceiling.
    """
    data, envelopes = _ahlt_decrypt_if_enveloped(data, vault_key=vault_key)
    if is_zstd_frame(data):
        data = decompress_zstd_stream(data, max_output_size=max_zstd_output_size)
        envelopes = [*envelopes, Envelope.ZSTD]
    return data, envelopes


class SqliteSource(AsyncClosing):
    """A connection to plain SQLite content, the common tail of all six
    sources in this module's docstring.

    Build one with the async classmethod factories (``from_bytes``,
    ``from_raw_store``, ...). Usable as ``async with``, or ``await
    close()`` directly. Except for a ``from_raw_store`` of a plain
    ``LocalFsStore`` file (opened read-only in place), the content is a
    private temp copy, removed on close; the copy's connection is writable
    (see ``from_bytes``), so the source repository is never modified.
    """

    def __init__(
        self,
        connection: aiosqlite.Connection,
        *,
        path: str | None = None,
        tmp_dir: tempfile.TemporaryDirectory[str] | None = None,
    ) -> None:
        """Takes ownership of ``connection`` and of the scratch ``path`` file
        or ``tmp_dir`` behind it, all released by ``close()``; the factories
        are the usual way to build one."""
        self.connection = connection
        self._path = path
        self._tmp_dir = tmp_dir
        self._closed = False

    @property
    def path(self) -> str | None:
        """The temp file's path for a source built by ``from_bytes`` or
        ``from_enveloped_bytes``, else ``None``. For a caller that must
        inspect the file's raw bytes, which no query against ``connection``
        can express."""
        return self._path

    @classmethod
    async def from_bytes(cls, data: bytes | bytearray) -> Self:
        """Materialize plain (post-``peel``) SQLite ``data`` to a private temp
        file and open a writable connection to it, so ``apply_index_hint`` can
        build an index. ``close()`` unlinks the file.

        Raises:
            ResourceLimitExceededError: ``data`` doesn't fit in the system
                temp directory with ``disk_space``'s reserve left free.
        """
        dir_path = Path(tempfile.gettempdir())
        path, _ = await _write_temp_file(lambda f: _write_reserved(data, f, dir_path), dir_path=dir_path)
        return cls(await _connect_temp_file(path), path=path)

    @classmethod
    async def from_enveloped_bytes(
        cls,
        data: bytes | bytearray,
        *,
        vault_key: bytes | None = None,
        max_output_size: int | None,
        tmp_dir: str | Path | None = None,
        what: str,
    ) -> tuple[Self, list[Envelope]]:
        """Like ``from_bytes()``, but for ``data`` that may still be
        ``aHlT``/zstd enveloped (an embedded ``ObjectDB`` slice, a SaaS
        service-level DB snapshot, ``version.db.zst``), streaming the
        decompressed result straight into the temp file. Returns
        ``(source, envelopes_stripped)``.

        Args:
            max_output_size: Mandatory fallback ceiling for a zstd frame
                that declares no size (a declared size sizes the disk-space
                check and caps the write instead; see
                ``format.compression.iter_decompressed_zstd``). ``None``
                with no declared size caps the write at the free space
                above ``disk_space``'s reserve instead.
            tmp_dir: Directory for the temp file; the system temp directory
                when omitted.
            what: Names ``data``'s source in error messages.

        Raises:
            DataCorruptError: ``data`` matches the zstd/``aHlT`` magic but
                doesn't decode, is ``aHlT``-enveloped with no ``vault_key``,
                or a declared content size doesn't match what decompression
                produced.
            FormatError: ``data`` has the ``aHlT`` magic but is shorter
                than its 64-byte header.
            ResourceLimitExceededError: The declared size (or, absent one,
                ``max_output_size``) doesn't fit in ``tmp_dir`` with
                ``disk_space``'s reserve left free, or an unsized,
                uncapped write outgrows that space.
        """
        dir_path = Path(tmp_dir) if tmp_dir is not None else Path(tempfile.gettempdir())
        try:
            path, envelopes = await _write_temp_file(
                lambda f: _peel_into(data, f, vault_key=vault_key, max_output_size=max_output_size, dir_path=dir_path),
                dir_path=dir_path,
            )
        except (zstandard.ZstdError, KeyRequiredError) as exc:
            raise DataCorruptError(f"{what} failed to decompress: {exc}") from exc
        return cls(await _connect_temp_file(path), path=path), envelopes

    @classmethod
    async def from_raw_store(cls, store: ObjectStore, path: str) -> Self:
        """Open ``path`` from ``store`` for a caller that knows it is never
        enveloped (``db/<name>``, ``saas/*/db/saas_{version,snapshot}``).
        Delegates to ``open_sqlite``: a plain ``LocalFsStore`` file without a
        non-empty ``-wal`` opens read-only in place, anything else via a
        private materialized copy.

        Raises:
            ResourceLimitExceededError: The materialized copy doesn't fit in
                the system temp directory with ``disk_space``'s reserve left
                free.
        """
        return await cls._from_open_sqlite(store, path)

    @classmethod
    async def from_enveloped_store(
        cls, store: ObjectStore, path: str, *, vault_key: bytes | None, tmp_dir: str | Path | None = None, what: str
    ) -> Self:
        """Read and open ``path`` from ``store`` — for a source that may be
        ``aHlT``-enveloped (``copy_meta_file/*/target.db``,
        FORMAT-SPEC.md: Landing directory layout) and may have a real
        ``-wal``/``-shm`` sidecar. The main file and each sidecar are
        unwrapped independently, streaming into the temp file, with the
        decompressed size bounded by ``_MAX_TARGET_DB_DECOMPRESS_SIZE_FALLBACK``
        as ``from_enveloped_bytes`` bounds it by ``max_output_size``.

        Args:
            tmp_dir: Directory for the temp file; the system temp directory
                when omitted.
            what: Names ``path`` in error messages.

        Raises:
            DataCorruptError: As ``from_enveloped_bytes``, for the main
                file or a sidecar (including a missing ``vault_key``).
            FormatError: As ``from_enveloped_bytes``.
            ResourceLimitExceededError: As ``from_enveloped_bytes``.
        """
        dir_path = Path(tmp_dir) if tmp_dir is not None else Path(tempfile.gettempdir())

        def _peel_to_file(raw: bytes, dest: BinaryIO) -> None:
            _peel_into(
                raw,
                dest,
                vault_key=vault_key,
                max_output_size=_MAX_TARGET_DB_DECOMPRESS_SIZE_FALLBACK,
                dir_path=dir_path,
            )

        try:
            return await cls._from_open_sqlite(
                store, path, transform=_peel_to_file, tmp_dir=Path(tmp_dir) if tmp_dir is not None else None
            )
        except (zstandard.ZstdError, KeyRequiredError) as exc:
            raise DataCorruptError(f"{what} failed to decompress: {exc}") from exc

    @classmethod
    async def _from_open_sqlite(
        cls,
        store: ObjectStore,
        path: str,
        *,
        transform: Callable[[bytes, BinaryIO], None] | None = None,
        tmp_dir: Path | None = None,
    ) -> Self:
        conn, tmp = await open_sqlite(store, path, tmp_dir=tmp_dir, transform=transform)
        return cls(conn, tmp_dir=tmp)

    @override
    async def close(self) -> None:
        """Closes the connection and removes its temp file or directory. Safe
        to call more than once."""
        if self._closed:
            return
        self._closed = True
        await self.connection.close()
        await asyncio.to_thread(self._remove_scratch_files)

    def _remove_scratch_files(self) -> None:
        if self._path is not None:
            os.unlink(self._path)
            # Best-effort: a read-write connection may leave sidecars.
            for suffix in ("-wal", "-shm", "-journal"):
                with contextlib.suppress(OSError):
                    os.unlink(self._path + suffix)
        if self._tmp_dir is not None:
            self._tmp_dir.cleanup()


@contextlib.asynccontextmanager
async def close_on_error(source: SqliteSource) -> AsyncIterator[None]:
    """Closes ``source`` if the wrapped block raises, then re-raises, so a
    caller validating an already-open ``source`` doesn't leak it."""
    try:
        yield
    except BaseException:
        await source.close()
        raise
