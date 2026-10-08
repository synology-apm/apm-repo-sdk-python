"""``ls``/``export`` with ``--object-db-id`` through the real CLI on a
hand-built repository whose SaaS version has no object-name index, so
automatic discovery has nothing to use and only the manual override reaches
its objects."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from support.cli import invoke
from support.format_builders import (
    build_object_db,
)
from support.repo_builders import (
    chunk_it,
    version_spec_json,
    write_bucket,
    write_composition,
    write_connection_config,
    write_copy_target_version,
    write_file_map,
    write_repo_info,
    write_saas_snapshot_db,
    write_saas_version_db,
    write_vault_encryption_key_db,
    write_workload_config,
)
from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, detect_repository_layout
from synology_apm_repo.sdk.storage.local import LocalFsStore

_SYN_STREAM_ID = 41
_SYN_CCID = 6
_SYN_CONNECTION_ID = "conn-synthetic-2"
_SYN_STREAM_UUID = "synthetic-degraded-stream-2"
_SYN_UNRECOGNIZED_SUB_TYPE = "SOME_FUTURE_CONNECTOR_TYPE"


def _build_synthetic_degraded_repo(tmp_path: Path, *, session_id: int = 8) -> str:
    """A workload whose ``sub_type`` has no dispatch candidate and whose
    version carries no object-name index (no ``additional_meta``): the case
    ``--object-db-id`` exists for. Otherwise like
    ``tests/unit/sdk/test_api_raw_fallback.py``'s ``_build_degraded_saas_repo``.
    Returns the ObjectDB's ``object_db_id``, known by construction."""
    write_repo_info(tmp_path / "repo_info")
    (tmp_path / "link.key").write_bytes(b"")
    (tmp_path / ".fully_created").write_bytes(b"")
    write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    write_connection_config(tmp_path / "db" / "connection_config", [(_SYN_CCID, _SYN_CONNECTION_ID, 1)])
    write_workload_config(
        tmp_path / "db" / "workload_config",
        [(1, "synthetic-workload-uid-2", "GW", {"spec": {"workload_type": _SYN_UNRECOGNIZED_SUB_TYPE}})],
    )
    write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                1,
                1,
                _SYN_CCID,
                "synthetic-version-uid-2",
                "GW",
                _SYN_STREAM_UUID,
                _SYN_STREAM_UUID,
                "snap-uuid",
                1,
                0,
                version_spec_json(start_time=1767225600, status="COMPLETED"),
            )
        ],
    )

    stream_db_dir = tmp_path / "saas" / str(_SYN_CCID) / _SYN_STREAM_UUID / "db"
    write_saas_snapshot_db(stream_db_dir / "saas_snapshot", snapshots=[(1, "snap-uuid", 1, 1)])
    write_saas_version_db(stream_db_dir / "saas_version", target_type="GW", versions=[(1, 1, 1, 0)])

    payloads = [("v1_object_1", b"aaaaa"), ("v1_object_2", b"bbbbb"), ("v1_object_3", b"ccccc")]
    relative_rows = []
    cursor = 0
    content = b""
    for object_id, payload in payloads:
        relative_rows.append((object_id, cursor, len(payload)))
        content += payload
        cursor += len(payload)
    object_db_len = len(build_object_db(relative_rows))
    absolute_rows = [(oid, off + object_db_len, ln) for oid, off, ln in relative_rows]
    object_db_bytes = build_object_db(absolute_rows)
    saas_obj_content = object_db_bytes + content
    plaintexts = chunk_it(saas_obj_content)

    saas_obj_path = f"{_SYN_STREAM_UUID}/{_SYN_CONNECTION_ID}/1/saas_obj"
    write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _SYN_STREAM_ID, session_id, 64, len(plaintexts), 2)])
    write_composition(
        tmp_path / "@data" / "Composition", stream_id=_SYN_STREAM_ID, session_id=session_id, num_chunks=len(plaintexts)
    )
    write_bucket(tmp_path / "@data" / "Pool" / str(_SYN_STREAM_ID) / "0.buk", plaintexts)
    return f"{_SYN_STREAM_UUID}_0_{object_db_len}"


def test_object_db_id_cli_end_to_end_ls_and_export_against_a_synthetic_degraded_version(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    object_db_id = _build_synthetic_degraded_repo(repo_root)

    async def _resolve() -> tuple[int, int, str]:
        store = LocalFsStore(repo_root)
        (layout,) = catalog_repo_layouts(await detect_repository_layout(store))
        async with await DedupRepo.open(store, layout) as repo:
            all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
            [workload] = all_workloads
            [version] = await versions(repo, workload)
            return version.connection_config_id, workload.workload_id, version.version_uid

    ccid, workload_id, version_uid = asyncio.run(_resolve())

    ref = f"{repo_root}#cat:{ccid}/wl:{workload_id}/ver:{version_uid}"

    ls_result = invoke(["--json", "ls", ref, "--object-db-id", object_db_id])
    rows = json.loads(ls_result.stdout)
    assert len(rows) == 3  # the 3 payloads the builder writes
    first_object_name = rows[0]["name"]

    export_ref = f"{ref}/{first_object_name}"
    dst = tmp_path / "object.bin"
    invoke(["export", export_ref, "-o", str(dst), "--object-db-id", object_db_id])
    assert dst.exists()
    assert dst.stat().st_size == rows[0]["size"]
