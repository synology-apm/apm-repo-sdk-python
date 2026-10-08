"""Writers the ``test_units_verify_reachable_*`` files share: a composition
with damage knobs, ``db/file_meta`` sizes, one resolvable FS workload+version
(``write_fs_workload``) and its PC/PS counterpart (``write_pcps_workload``),
and ``open_vault_repo``, which opens the result."""

from __future__ import annotations

import zlib
from pathlib import Path

from support.format_builders import composition_header_bytes, record_head_bytes
from support.repo_builders import (
    open_db,
    version_spec_json,
    write_connection_config,
    write_copy_target_file,
    write_copy_target_version,
    write_copy_target_version_meta,
    write_pcps_file_meta,
    write_repo_info,
    write_target_db_with_version_id,
    write_workload_config,
)
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore


def write_damaged_composition(
    root: Path,
    *,
    stream_id: int,
    session_id: int,
    entries: bytes,
    corrupt_header: bool = False,
    corrupt_record_head: bool = False,
    on_disk_entries: bytes | None = None,
    trailer: bytes = b"",
) -> None:
    """``write_composition_entries`` with damage knobs: ``corrupt_header``/
    ``corrupt_record_head`` break the ``cMpS``/``Mu`` magic, and
    ``on_disk_entries`` (default: ``entries``) are the bytes written, which
    may differ from the ``entries`` ``map_crc`` covers; ``trailer`` follows
    them. Together they express a corrupted chunk-map array that a Redundancy
    blob in ``trailer`` can recover."""
    map_num = len(entries) // 20
    head = bytearray(record_head_bytes(map_num=map_num, map_crc=zlib.crc32(entries) & 0xFFFFFFFF))
    if corrupt_record_head:
        head[0] ^= 0xFF  # bad "Mu" magic
    comp_header = bytearray(composition_header_bytes())
    if corrupt_header:
        comp_header[0] ^= 0xFF  # bad "cMpS" magic
    path = root / str(stream_id) / f"{session_id}.com" / "c0"
    path.parent.mkdir(parents=True, exist_ok=True)
    on_disk = on_disk_entries if on_disk_entries is not None else entries
    path.write_bytes(bytes(comp_header) + bytes(head) + on_disk + trailer)


def write_file_meta_sizes(path: Path, rows: list[tuple[str, int]]) -> None:
    """``db/file_meta``'s ``file_size`` column. Without a matching row the
    file's size is unknown (``None``), and ``composition_extents_for_version``
    yields no extent for it rather than raising."""
    conn = open_db(path)
    conn.execute("CREATE TABLE IF NOT EXISTS file_meta(path TEXT PRIMARY KEY, file_size INTEGER)")
    conn.executemany("INSERT INTO file_meta VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def write_fs_workload(
    tmp_path: Path,
    *,
    workload_id: int,
    version_uid: str,
    target_id: str,
    meta_dirname: str,
    dedup_version_id: int,
    dedup_img_size: int | None = None,
    write_target_db: bool = True,
) -> str:
    """Register one resolvable FS workload+version in
    ``workload_config``/``copy_target_version``/``copy_target_version_meta``
    plus its own ``copy_meta_file/<meta_dirname>/target.db`` — everything
    ``composition_extents_for_version``'s FS branch needs short of the
    ``file_map`` row itself (a caller adds that separately, so two
    versions built this way can share one row's target composition/bucket).
    Returns the ``dedup.img`` path this version's ``file_map`` row must use.

    ``dedup_img_size``, when given, also registers that path's
    ``file_size`` (see ``write_file_meta_sizes``).

    ``write_target_db=False`` leaves the catalog rows naming a ``target.db``
    that is never written: a version whose metadata claims more than exists.
    """
    fs_spec: dict[str, object] = {"namespace": "ns-a", "spec": {"workload_type": "FS", "workload_name": target_id}}
    write_workload_config(tmp_path / "db" / "workload_config", [(workload_id, f"{target_id}-uid", "FS", fs_spec)])
    write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                workload_id * 100,
                workload_id,
                1,
                version_uid,
                "FS",
                target_id,
                "",
                "",
                0,
                0,
                version_spec_json(1786000000, status="COMPLETED"),
            )
        ],
    )
    write_copy_target_version_meta(
        tmp_path / "db" / "copy_target_version_meta",
        [(version_uid, f"/pv/copy_meta_file/{meta_dirname}", ["target.db", "version.db.zst"], 1)],
    )
    if write_target_db:
        write_target_db_with_version_id(tmp_path / "copy_meta_file" / meta_dirname / "target.db", dedup_version_id)
        # meta_filenames above also names version.db.zst; write it so the
        # meta directory holds every file its catalog row claims.
        (tmp_path / "copy_meta_file" / meta_dirname / "version.db.zst").touch()
    dedup_img_path = f"{target_id}/{dedup_version_id}/dedup.img"
    if dedup_img_size is not None:
        write_file_meta_sizes(tmp_path / "db" / "file_meta", [(dedup_img_path, dedup_img_size)])
    return dedup_img_path


def write_pcps_workload(
    tmp_path: Path,
    *,
    workload_id: int,
    version_uid: str,
    target_id: str,
    fid: int,
    src_file_path: str,
    disk_size: int,
) -> None:
    """Register one resolvable PC/PS workload+version with one disk
    fragment. PC/PS has no ``target.db``: it resolves off
    ``copy_target_version``/``copy_target_file`` joined with
    ``db/file_meta``."""
    pcps_spec: dict[str, object] = {"namespace": "ns-a", "spec": {"workload_type": "PC", "workload_name": target_id}}
    write_workload_config(tmp_path / "db" / "workload_config", [(workload_id, f"{target_id}-uid", "PC", pcps_spec)])
    version_id = workload_id * 100
    write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                version_id,
                workload_id,
                1,
                version_uid,
                "PC",
                target_id,
                "",
                "",
                0,
                0,
                version_spec_json(1786000000, status="COMPLETED"),
            )
        ],
    )
    write_copy_target_file(tmp_path / "db" / "copy_target_version", [(version_id, fid)])
    write_pcps_file_meta(tmp_path / "db" / "file_meta", [(fid, src_file_path, disk_size)])


async def open_vault_repo(root: Path) -> DedupRepo:
    """Writes ``repo_info`` and one connection (``conn-a``, id 1) under
    ``root`` and opens it as a vault ``DedupRepo``; the caller closes it."""
    write_repo_info(root / "repo_info")
    write_connection_config(root / "db" / "connection_config", [(1, "conn-a", 1)])
    store = LocalFsStore(root)
    layout = RepoLayout(kind=RepoKind.VAULT, repo_root="")
    return await DedupRepo.open(store, layout)
