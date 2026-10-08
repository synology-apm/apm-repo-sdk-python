"""The synthetic VM repository root the ``units.device`` unit tests build
(``test_units_device.py``, ``test_units_content_local_file.py``)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from support.model_factories import make_version
from support.repo_builders import (
    write_bucket,
    write_composition,
    write_file_map,
    write_repo_info,
    write_vault_encryption_key_db,
)
from synology_apm_repo.sdk.catalog.version import Version, VersionMeta
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from synology_apm_repo.sdk.storage.local import LocalFsStore

VM_STREAM_ID = 5
DISK_PLAINTEXT = b"\x55\xaa" + b"\x00" * 4094  # a fake but recognizable "MBR"
assert len(DISK_PLAINTEXT) == 4096


def write_repo_info_and_vault_key_db(tmp_path: Path) -> tuple[LocalFsStore, RepoLayout]:
    """Writes ``repo_info`` and an all-``NoEncryption`` vault_encryption_key
    db, returning ``(store, layout)`` for ``DedupRepo.open()``."""
    write_repo_info(tmp_path / "repo_info")
    write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    return LocalFsStore(tmp_path), RepoLayout(kind=RepoKind.VAULT, repo_root="")


def write_target_db(
    path: Path,
    *,
    version_id: int = 1,
    config_device_id: int = 1,
    device_uuid: str = "device-uuid",
    host_name: str = "my-vm",
    os_name: str = "Windows",
    objects: list[tuple[int, int, str, str, str, int, int]],
) -> None:
    """``objects``: (object_id, data_format, file_path, src_file_path, temp_postfix, dedup_object, file_size)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE version_table(id INTEGER PRIMARY KEY, version_id INTEGER, data_format INTEGER, "
        "status INTEGER, folder_name TEXT)"
    )
    conn.execute("INSERT INTO version_table VALUES (1, ?, 1, 1, 'folder')", (version_id,))
    conn.execute(
        "CREATE TABLE device_table(device_id INTEGER PRIMARY KEY, version_id INTEGER, config_device_id INTEGER, "
        "device_uuid TEXT, host_name TEXT, os_name TEXT)"
    )
    conn.execute(
        "INSERT INTO device_table VALUES (1, ?, ?, ?, ?, ?)",
        (version_id, config_device_id, device_uuid, host_name, os_name),
    )
    conn.execute(
        "CREATE TABLE object_table(object_id INTEGER PRIMARY KEY, version_id INTEGER, config_device_id INTEGER, "
        "data_format INTEGER, file_path TEXT, src_file_path TEXT, temp_postfix TEXT, dedup_object INTEGER, "
        "file_size INTEGER)"
    )
    conn.executemany(
        "INSERT INTO object_table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (object_id, version_id, config_device_id, data_format, file_path, src_file_path, temp_postfix, dedup, size)
            for object_id, data_format, file_path, src_file_path, temp_postfix, dedup, size in objects
        ],
    )
    conn.commit()
    conn.close()


def build_vm_repo(
    tmp_path: Path,
    *,
    stream_id: int = VM_STREAM_ID,
    session_id: int = 9,
    extra_objects: list[tuple[int, int, str, str, str, int, int]] | None = None,
) -> None:
    write_repo_info_and_vault_key_db(tmp_path)
    src_path = "VM-uid/ActiveBackup_2026-01-01/my-vm/disk.img"
    write_file_map(tmp_path / "db" / "file_map", [(src_path, stream_id, session_id, 64, 1, 2)])
    objects = [(1, 1, "ActiveBackup_2026-01-01/my-vm/disk.img", src_path, "", 1, 4096)]
    if extra_objects:
        objects += extra_objects
    write_target_db(tmp_path / "copy_meta_file" / "VM_uid1" / "target.db", objects=objects)
    write_composition(tmp_path / "@data" / "Composition", stream_id=stream_id, session_id=session_id)
    write_bucket(tmp_path / "@data" / "Pool" / str(stream_id) / "0.buk", [DISK_PLAINTEXT], stream_id=VM_STREAM_ID)


def vm_version(
    target_meta_path: str = "/pv/20/copy_meta_file/VM_uid1",
    *,
    meta_filenames: tuple[str, ...] = ("target.db",),
    meta_status: int = 1,
) -> Version:
    return make_version(
        target_id="VM-uid",
        meta=VersionMeta(target_meta_path=target_meta_path, meta_filenames=meta_filenames, status=meta_status),
    )
