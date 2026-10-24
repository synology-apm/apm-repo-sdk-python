"""Opening a SQLite database inside an ``ObjectStore``, for every backend.

- **Fast path** (``LocalFsStore`` only, the one place the SDK maps store
  paths onto real files): with no non-empty ``-wal`` sidecar, open the file
  read-only through an immutable ``file:`` URI.
- **Slow path** (every other store, a non-empty ``-wal``, or a
  ``transform``): copy the file and any ``-wal``/``-shm`` sidecars into a
  temp directory and open that copy read-write, so SQLite replays the WAL
  without touching the store. A non-empty ``-shm`` alone doesn't trigger it.

``apply_index_hint`` lives here because only the slow path's private copy
can be given an index.

``table_columns`` and ``apply_index_hint`` ask each open connection only
once per table (per hint): an open database's tables keep their columns,
and an applied hint only adds an index.
"""

from __future__ import annotations

import asyncio
import tempfile
import weakref
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import BinaryIO

import aiosqlite

from .._util.closing import close_preserving, run_to_completion, shield_or_undo
from ..errors import NotFoundError
from .base import ObjectStore
from .disk_space import reserve_disk_space
from .local import LocalFsStore

_WAL_SUFFIX = "-wal"
_SHM_SUFFIX = "-shm"


class _KnownSchema:
    """What one connection has already been asked: each table's columns,
    and the index hints applied (or found already covered)."""

    __slots__ = ("columns", "hints")

    def __init__(self) -> None:
        self.columns: dict[str, frozenset[str]] = {}
        self.hints: set[tuple[str, tuple[str, ...]]] = set()


_known_schemas: weakref.WeakKeyDictionary[aiosqlite.Connection, _KnownSchema] = weakref.WeakKeyDictionary()


def _known_schema(conn: aiosqlite.Connection) -> _KnownSchema:
    known = _known_schemas.get(conn)
    if known is None:
        known = _known_schemas[conn] = _KnownSchema()
    return known


async def _close(connection: aiosqlite.Connection | None) -> None:
    if connection is not None:
        await connection.close()


async def connect(uri: str) -> aiosqlite.Connection:
    """``aiosqlite.connect(uri, uri=True)``, closing the connection if it
    opens anyway after the caller failed or was cancelled mid-connect: its
    non-daemon thread would otherwise block interpreter exit."""
    return await shield_or_undo(aiosqlite.connect(uri, uri=True), _close)


async def open_sqlite(
    store: ObjectStore,
    path: str,
    *,
    tmp_dir: Path | None = None,
    transform: Callable[[bytes, BinaryIO], None] | None = None,
    listing: Mapping[str, int | None] | None = None,
) -> tuple[aiosqlite.Connection, tempfile.TemporaryDirectory[str] | None]:
    """Open an ``aiosqlite`` connection to ``path`` within ``store``,
    read-only on the fast path, read-write on the slow path's private copy
    (see the module docstring).

    Args:
        store: The store holding ``path``.
        path: Store-relative path of the SQLite file.
        tmp_dir: Parent directory for the slow path's temp directory
            (default: the system temp directory).
        transform: Applied independently to each file's raw bytes (main file
            and any ``-wal``/``-shm``) before materializing, and forces the
            slow path. It receives the raw bytes and an open destination
            file and writes the result into it, so a decompressing transform
            never holds its whole output in memory. It reserves its own
            disk space (``disk_space``); an untransformed copy is reserved
            here.
        listing: ``path``'s directory as already listed (entry name -> the
            size the listing reported, or ``None``), answering for the
            ``-wal``/``-shm`` sidecars with no request; without it each is
            probed with ``exists``.

    Returns:
        ``(connection, materialized_tmp_dir)``. ``materialized_tmp_dir`` is
        ``None`` on the fast path; on the slow path it is the
        ``TemporaryDirectory`` backing the copy, which the caller must keep
        referenced while ``connection`` is used and then ``.cleanup()``.

    Raises:
        NotFoundError: ``path`` does not exist, on either path.
        ResourceLimitExceededError: A file of the slow path's copy doesn't
            fit in ``tmp_dir`` with ``disk_space``'s reserve left free.
    """
    base_name = Path(path).name

    async def _sidecar_present(suffix: str) -> bool:
        if listing is not None:
            return base_name + suffix in listing
        return await store.exists(path + suffix)

    async def _wal_nonempty() -> bool:
        # A listing taken earlier still answers: the repository is read as a snapshot.
        if not await _sidecar_present(_WAL_SUFFIX):
            return False
        listed_size = listing.get(base_name + _WAL_SUFFIX) if listing is not None else None
        return (listed_size if listed_size is not None else await store.size(path + _WAL_SUFFIX)) > 0

    # Only the LocalFsStore fast path needs the WAL-size check; skipping it elsewhere saves a round trip.
    if transform is None and isinstance(store, LocalFsStore) and not await _wal_nonempty():
        real_path = store.local_path(path)
        # SQLite would fail the open as an unreadable database; to this
        # layer a missing file is absent, as the slow path's read says.
        if not await asyncio.to_thread(real_path.exists):
            raise NotFoundError("no such file", ref=path)
        uri = f"file:{real_path}?mode=ro&immutable=1"
        return await connect(uri), None

    tmp = tempfile.TemporaryDirectory(dir=tmp_dir)
    try:
        dest_dir = Path(tmp.name)

        def _write_dest(dest_path: Path, raw: bytes) -> None:
            if transform is not None:
                with open(dest_path, "wb") as f:
                    transform(raw, f)
            else:
                with reserve_disk_space(dest_dir, len(raw)):
                    dest_path.write_bytes(raw)

        async def _materialize(src_path: str, dest_name: str) -> None:
            raw = await store.read(src_path)
            # A cancelled writer still waits for its thread: tmp.cleanup()
            # can't remove a file it still has open (Windows).
            await run_to_completion(asyncio.to_thread(_write_dest, dest_dir / dest_name, raw))

        async def _materialize_sidecar_if_present(suffix: str) -> None:
            if await _sidecar_present(suffix):
                await _materialize(path + suffix, base_name + suffix)

        # return_exceptions=True: every task finishes before tmp.cleanup() can race a writer.
        results = await asyncio.gather(
            _materialize(path, base_name),
            *(_materialize_sidecar_if_present(suffix) for suffix in (_WAL_SUFFIX, _SHM_SUFFIX)),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result

        # Read-write on our private copy, so apply_index_hint() can build an index.
        # ``rw`` not ``rwc``: an unresolvable path must fail here, not open an empty database.
        conn = await connect(f"file:{dest_dir / base_name}?mode=rw")
    except BaseException as exc:
        await close_preserving(exc, [lambda: run_to_completion(asyncio.to_thread(tmp.cleanup))])
        raise
    return conn, tmp


async def _index_leading_columns(conn: aiosqlite.Connection, table: str) -> list[list[str]]:
    """Every existing index on ``table`` as its ordered column names
    (``PRAGMA index_info`` ``seqno`` order). Names are bound, not spliced:
    an index name comes from the database itself."""
    cursor = await conn.execute("SELECT name FROM pragma_index_list(?)", (table,))
    index_names = [row[0] for row in await cursor.fetchall()]
    result: list[list[str]] = []
    for index_name in index_names:
        info_cursor = await conn.execute("SELECT name FROM pragma_index_info(?) ORDER BY seqno", (index_name,))
        result.append([row[0] for row in await info_cursor.fetchall()])
    return result


async def table_columns(conn: aiosqlite.Connection, table: str) -> frozenset[str]:
    """``table``'s column names (empty when it doesn't exist), from ``PRAGMA
    table_info`` once per connection."""
    known = _known_schema(conn)
    columns = known.columns.get(table)
    if columns is None:
        cursor = await conn.execute(f"PRAGMA table_info({table})")
        columns = known.columns[table] = frozenset(row[1] for row in await cursor.fetchall())
    return columns


async def apply_index_hint(conn: aiosqlite.Connection, table: str, columns: Sequence[str]) -> None:
    """Declare that queries on ``table`` will filter or sort by ``columns``;
    a hint, not a guarantee. Skips if an existing index already covers
    ``columns`` as a leading prefix, otherwise tries ``CREATE INDEX IF NOT
    EXISTS``. A read-only connection's "readonly database" error is swallowed
    (the hint does nothing); any other error propagates, and the hint is
    tried again on the next call.
    """
    wanted = list(columns)
    known = _known_schema(conn)
    key = (table, tuple(wanted))
    if key in known.hints:
        return
    existing = await _index_leading_columns(conn, table)
    if not any(cols[: len(wanted)] == wanted for cols in existing):
        index_name = f"_synology_apm_repo_idx_{table}_{'_'.join(wanted)}"
        col_list = ", ".join(wanted)
        try:
            await conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({col_list})")
            await conn.commit()
        except aiosqlite.OperationalError as exc:
            if "readonly database" not in str(exc):
                raise
    known.hints.add(key)
