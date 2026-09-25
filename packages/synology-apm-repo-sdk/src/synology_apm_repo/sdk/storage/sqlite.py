"""SQLite access helper — a free function, not an ``ObjectStore`` Protocol
method, since WAL handling is shared logic every backend needs
identically.

Two paths:

- **Fast path**: no non-zero ``-wal`` sidecar present -> open directly via
  a read-only, immutable ``file:`` URI. Only valid for ``LocalFsStore`` —
  the only place in the SDK that assumes a store's paths map onto real
  filesystem paths, which is why this is a free function rather than a
  method every backend must implement; S3/Azure stores always take the
  slow path.
- **Slow path**: a non-zero ``-wal`` sidecar exists (real observed case:
  ``copy_meta_file/target.db``) -> copy the main file plus any present
  ``-wal``/``-shm`` sidecars into a temp directory and open *that* copy
  read-write (letting SQLite's own recovery replay the WAL), so the
  original store's bytes are never touched. Only ``-wal``'s size gates
  this decision — a non-zero ``-shm`` alone does not trigger it.

The read-only/read-write split between the two is the whole reason
``apply_index_hint`` lives here rather than in ``storage/table.py``: only
a connection onto a copy we own can be given an index, and this module is
where that distinction is made.
"""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path

import aiosqlite

from .base import ObjectStore
from .local import LocalFsStore

_WAL_SUFFIX = "-wal"
_SHM_SUFFIX = "-shm"


async def open_sqlite(
    store: ObjectStore,
    path: str,
    *,
    tmp_dir: Path | None = None,
    transform: Callable[[bytes], bytes] | None = None,
) -> tuple[aiosqlite.Connection, tempfile.TemporaryDirectory[str] | None]:
    """Open an ``aiosqlite`` connection to ``path`` within ``store`` — read-only
    on the fast path, read-write on the slow path's own private materialized
    copy. The slow path is taken whenever ``transform`` is given, ``store``
    isn't a ``LocalFsStore``, or a non-zero ``-wal`` sidecar is present;
    the fast path is the ``LocalFsStore``-only case with none of those.

    Returns ``(connection, materialized_tmp_dir)``. ``materialized_tmp_dir``
    is ``None`` on the fast path (nothing extra was created); on the slow
    path it is the ``tempfile.TemporaryDirectory`` backing the
    materialized copy — the caller must keep a reference to it for as long
    as ``connection`` is used, and should call ``.cleanup()`` (or simply let
    it go out of scope) once done.

    ``transform``, when given, is applied independently to each file's raw
    bytes (main file, and any ``-wal``/``-shm`` sidecar) before materializing,
    and forces the slow path regardless of WAL state.

    ``aiosqlite`` pins each connection to one dedicated thread for its
    lifetime, funnelling every statement — including ``close()`` —
    through it, so ``check_same_thread`` is unnecessary.
    """
    wal_path = path + _WAL_SUFFIX
    # The upfront WAL-size check only ever gates the fast path below, and
    # that path is itself restricted to LocalFsStore — so for any other
    # store this check's result could never change what happens next.
    # Skipping it saves a real network round trip per db open on S3/Azure
    # (the loop below still checks -wal/-shm individually to decide
    # whether to copy each sidecar into the materialized copy — that part
    # applies regardless of store).
    if transform is None and isinstance(store, LocalFsStore):
        needs_materialize = await store.exists(wal_path) and await store.size(wal_path) > 0
        if not needs_materialize:
            real_path = store.root / path
            uri = f"file:{real_path}?mode=ro&immutable=1"
            return await aiosqlite.connect(uri, uri=True), None

    tmp = tempfile.TemporaryDirectory(dir=tmp_dir)
    try:
        dest_dir = Path(tmp.name)
        base_name = Path(path).name

        async def _materialize(src_path: str, dest_name: str) -> None:
            raw = await store.read(src_path)
            if transform is not None:
                raw = await asyncio.to_thread(transform, raw)
            # Local file writes have no native async form (see storage/local.py) —
            # to_thread() for the same reason LocalFsStore uses it.
            await asyncio.to_thread((dest_dir / dest_name).write_bytes, raw)

        async def _materialize_sidecar_if_present(suffix: str) -> None:
            side_path = path + suffix
            if await store.exists(side_path):
                await _materialize(side_path, base_name + suffix)

        # Concurrent: each of these is its own network round trip on a
        # remote store. return_exceptions=True (not a bare gather(), not a
        # TaskGroup) waits for every task to finish before touching dest_dir
        # again, so a failing task can't race tmp.cleanup() below against a
        # sibling still writing -- and keeps the raised exception in its own
        # type, unlike TaskGroup's ExceptionGroup wrapping.
        results = await asyncio.gather(
            _materialize(path, base_name),
            *(_materialize_sidecar_if_present(suffix) for suffix in (_WAL_SUFFIX, _SHM_SUFFIX)),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result

        # Read-*write*, unlike the fast path above: this is our own private
        # copy in a temp directory this function created and will delete, so
        # nothing here can reach the store's bytes. Writable is what lets
        # apply_index_hint() actually build an index instead of silently
        # doing nothing and leaving every later query a full table scan.
        # ``rw`` rather than plain (``rwc``): a path SQLite cannot resolve must
        # still fail here, not open as a brand-new empty database whose
        # missing tables only surface later as a DataCorruptError.
        conn = await aiosqlite.connect(f"file:{dest_dir / base_name}?mode=rw", uri=True)
    except Exception:
        tmp.cleanup()
        raise
    return conn, tmp


async def _index_leading_columns(conn: aiosqlite.Connection, table: str) -> list[list[str]]:
    """Every existing index on ``table``, each as its own ordered list of
    column names (``PRAGMA index_info``'s own ``seqno`` order — the order
    that actually matters for whether a leading-column prefix match is
    valid)."""
    cursor = await conn.execute(f"PRAGMA index_list({table})")
    index_names = [row[1] for row in await cursor.fetchall()]
    result: list[list[str]] = []
    for index_name in index_names:
        info_cursor = await conn.execute(f"PRAGMA index_info({index_name})")
        result.append([row[2] for row in await info_cursor.fetchall()])
    return result


async def apply_index_hint(conn: aiosqlite.Connection, table: str, columns: Sequence[str]) -> None:
    """A pure declaration of intent — "queries against ``table`` will
    filter/sort by ``columns``" — never a command this is guaranteed to
    fulfil; callers never need to check whether it actually did anything.
    Safe to call even when ``columns`` is already known to be indexed
    (e.g. a schema-defined index or a PRIMARY KEY): the leading-prefix
    check below makes that case a cheap no-op, so callers apply this
    uniformly rather than reasoning per call site about whether it's
    needed.

    Checks whether an existing index already covers ``columns`` as a
    leading prefix; skips if so. Otherwise attempts ``CREATE INDEX IF NOT
    EXISTS`` and lets a genuinely read-only connection's own answer settle
    whether that's even possible: SQLite raises ``OperationalError:
    attempt to write a readonly database`` synchronously and safely
    against a ``mode=ro`` connection, caught here and treated as "the
    hint quietly did nothing" — the caller's own query afterward just
    costs a full scan. Any other error still propagates.
    """
    existing = await _index_leading_columns(conn, table)
    wanted = list(columns)
    if any(cols[: len(wanted)] == wanted for cols in existing):
        return
    index_name = f"_synology_apm_repo_idx_{table}_{'_'.join(wanted)}"
    col_list = ", ".join(wanted)
    try:
        await conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({col_list})")
        await conn.commit()
    except aiosqlite.OperationalError as exc:
        if "readonly database" not in str(exc):
            raise
