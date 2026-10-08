"""``Session`` → ``Catalog.provider`` falling back to ``RawObjectProvider``
end to end, unmocked, on a hand-built repository whose SaaS workload has an
unrecognized ``sub_type``. Each link has its own faked unit test
(``test_api_catalog.py``, ``test_units_dispatch_saas.py``,
``test_units_saas_raw_object.py``); this one proves they connect."""

from __future__ import annotations

from pathlib import Path

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
from synology_apm_repo.sdk import api
from synology_apm_repo.sdk.units.saas.raw_object import RawObjectProvider

_STREAM_ID = 40
_CCID = 5
_CONNECTION_ID = "conn-synthetic"
_STREAM_UUID = "synthetic-degraded-stream"
_UNRECOGNIZED_SUB_TYPE = "SOME_FUTURE_CONNECTOR_TYPE"


def _build_degraded_saas_repo(tmp_path: Path, *, session_id: int = 7) -> None:
    """One SaaS workload with a 3-object ``ObjectDB`` whose ``sub_type`` has
    no dispatch candidate, so it resolves to ``RawObjectProvider``."""
    write_repo_info(tmp_path / "repo_info")
    (tmp_path / "link.key").write_bytes(b"")
    (tmp_path / ".fully_created").write_bytes(b"")
    write_vault_encryption_key_db(tmp_path / "db" / "vault_encryption_key")
    write_connection_config(tmp_path / "db" / "connection_config", [(_CCID, _CONNECTION_ID, 1)])
    write_workload_config(
        tmp_path / "db" / "workload_config",
        [(1, "synthetic-workload-uid", "GW", {"spec": {"workload_type": _UNRECOGNIZED_SUB_TYPE}})],
    )

    stream_db_dir = tmp_path / "saas" / str(_CCID) / _STREAM_UUID / "db"
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

    write_copy_target_version(
        tmp_path / "db" / "copy_target_version",
        [
            (
                1,
                1,
                _CCID,
                "synthetic-version-uid",
                "GW",
                _STREAM_UUID,
                _STREAM_UUID,
                "snap-uuid",
                1,
                0,
                version_spec_json(
                    start_time=1767225600,
                    additional_meta={
                        "object_db_id": f"{_STREAM_UUID}_0_{object_db_len}",
                        "db_object_ids": {
                            "db_objects": [
                                {"name": "obj_1", "object_id": "v1_object_1"},
                                {"name": "obj_2", "object_id": "v1_object_2"},
                                {"name": "obj_3", "object_id": "v1_object_3"},
                            ]
                        },
                    },
                    status="COMPLETED",
                ),
            )
        ],
    )

    saas_obj_path = f"{_STREAM_UUID}/{_CONNECTION_ID}/1/saas_obj"
    write_file_map(tmp_path / "db" / "file_map", [(saas_obj_path, _STREAM_ID, session_id, 64, len(plaintexts), 2)])
    write_composition(
        tmp_path / "@data" / "Composition", stream_id=_STREAM_ID, session_id=session_id, num_chunks=len(plaintexts)
    )
    write_bucket(tmp_path / "@data" / "Pool" / str(_STREAM_ID) / "0.buk", plaintexts)


async def test_catalog_provider_falls_back_to_raw_object_provider_via_the_object_name_index(
    tmp_path: Path,
) -> None:
    """A SaaS workload with an unrecognized ``sub_type`` resolves to
    ``RawObjectProvider``, which lists its 3 objects by name
    (``obj_1``..``obj_3``) and reads their content."""
    repo_root = tmp_path / "repo"
    _build_degraded_saas_repo(repo_root)
    async with api.Session() as session:
        [repo] = await session.open(repo_root)
        catalogs = await repo.catalogs()
        workload_pairs = [(c, w) for c in catalogs for w in await c.workloads()]
        [(catalog, workload)] = workload_pairs
        [version] = await catalog.versions(workload)

        provider = await catalog.provider(version)
        assert isinstance(provider, RawObjectProvider)
        nodes = await provider.children(provider.root())
        assert {n.name for n in nodes} == {"obj_1", "obj_2", "obj_3"}

        [obj_1] = [n for n in nodes if n.name == "obj_1"]
        content = (await provider.unit(obj_1)).content
        assert await content.read(0, content.size or 0) == b"aaaaa"
