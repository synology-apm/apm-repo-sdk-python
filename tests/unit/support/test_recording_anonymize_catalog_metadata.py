"""Unit tests for ``tests/support/recording/anonymize_catalog_metadata.py``'s
hash-derived anonymization, against SQLite bytes built in-test. The ``anon``
fixture gives each test an empty ``_VAULT_KEY_CACHE``."""

from __future__ import annotations

import base64
import json
import re
import sqlite3
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from support.recording import anonymize_catalog_metadata
from support.recording.fixture_store import load_fixture_text, write_fixture_text
from synology_apm_repo.sdk.format.crypto import decrypt_version_spec


@pytest.fixture
def anon(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.setattr(anonymize_catalog_metadata, "_VAULT_KEY_CACHE", {})
    return anonymize_catalog_metadata


def _build_db(ddl: list[str], inserts: list[tuple[str, tuple[Any, ...]]]) -> bytes:
    conn = sqlite3.connect(":memory:")
    try:
        for stmt in ddl:
            conn.execute(stmt)
        for stmt, params in inserts:
            conn.execute(stmt, params)
        conn.commit()
        return conn.serialize()
    finally:
        conn.close()


def _query(data: bytes, sql: str) -> list[tuple[Any, ...]]:
    conn = sqlite3.connect(":memory:")
    try:
        conn.deserialize(data)
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _workload_config_db(workload_spec: dict[str, Any]) -> bytes:
    return _build_db(
        ["CREATE TABLE workload_config (workload_id INTEGER, workload_spec TEXT)"],
        [("INSERT INTO workload_config VALUES (?, ?)", (1, json.dumps(workload_spec)))],
    )


#: The ``device_name`` placeholder for ``"real-device-01"``: its
#: ``int(sha256(...).hexdigest(), 16) % 65536`` as 4 hex digits.
_REAL_DEVICE_01_PLACEHOLDER = "CORP-PC-85f2"


def test_registered_field_replaced_sibling_untouched(anon: ModuleType) -> None:
    resolved: dict[str, str] = {}
    data = _workload_config_db({"spec": {"workload_name": "real-device-01", "unregistered_field": "should-stay"}})

    anon._process_sqlite_bytes(data, resolved, write=False)
    written = anon._process_sqlite_bytes(data, resolved, write=True)

    (spec_json,) = _query(written, "SELECT workload_spec FROM workload_config")[0]
    spec = json.loads(spec_json)
    assert spec["spec"]["workload_name"] == _REAL_DEVICE_01_PLACEHOLDER
    assert spec["spec"]["unregistered_field"] == "should-stay"


def test_same_real_value_in_json_and_path_gets_identical_placeholder(anon: ModuleType) -> None:
    resolved: dict[str, str] = {}
    data = _build_db(
        [
            "CREATE TABLE workload_config (workload_id INTEGER, workload_spec TEXT)",
            "CREATE TABLE file_map (id INTEGER, path TEXT)",
        ],
        [
            (
                "INSERT INTO workload_config VALUES (?, ?)",
                (1, json.dumps({"spec": {"workload_name": "real-device-01"}})),
            ),
            ("INSERT INTO file_map VALUES (?, ?)", (1, "/vol1/real-device-01/backup.img")),
        ],
    )

    anon._process_sqlite_bytes(data, resolved, write=False)
    written = anon._process_sqlite_bytes(data, resolved, write=True)

    (spec_json,) = _query(written, "SELECT workload_spec FROM workload_config")[0]
    (path,) = _query(written, "SELECT path FROM file_map")[0]
    placeholder = json.loads(spec_json)["spec"]["workload_name"]
    assert placeholder == _REAL_DEVICE_01_PLACEHOLDER
    assert path == f"/vol1/{placeholder}/backup.img"


def test_same_real_value_stable_across_separate_runs(anon: ModuleType) -> None:
    """Two runs sharing no state (each with its own ``resolved``) derive the
    same placeholder from the real value's hash alone."""
    data = _workload_config_db({"spec": {"workload_name": "real-device-01"}})

    first_resolved: dict[str, str] = {}
    anon._process_sqlite_bytes(data, first_resolved, write=False)
    first_written = anon._process_sqlite_bytes(data, first_resolved, write=True)
    first_row = _query(first_written, "SELECT workload_spec FROM workload_config")[0][0]
    first_placeholder = json.loads(first_row)["spec"]["workload_name"]

    second_resolved: dict[str, str] = {}
    anon._process_sqlite_bytes(data, second_resolved, write=False)
    second_written = anon._process_sqlite_bytes(data, second_resolved, write=True)
    second_row = _query(second_written, "SELECT workload_spec FROM workload_config")[0][0]
    second_placeholder = json.loads(second_row)["spec"]["workload_name"]

    assert first_placeholder == second_placeholder == _REAL_DEVICE_01_PLACEHOLDER


def test_same_real_value_under_different_categories_gets_correlated_not_reshaped(anon: ModuleType) -> None:
    """A real value in two fields of different categories (a workload name
    that is also a site name) gets the same placeholder in both, so it
    still reads as one thing, even though one field's placeholder then has
    the other category's shape."""
    resolved: dict[str, str] = {}
    data = _workload_config_db(
        {
            "spec": {"workload_name": "shared-real-value"},
            "status": {"entity_meta": {"spec": {"site_info": {"site_name": "shared-real-value"}}}},
        }
    )

    anon._process_sqlite_bytes(data, resolved, write=False)
    written = anon._process_sqlite_bytes(data, resolved, write=True)

    (spec_json,) = _query(written, "SELECT workload_spec FROM workload_config")[0]
    spec = json.loads(spec_json)
    device_placeholder = spec["spec"]["workload_name"]
    site_placeholder = spec["status"]["entity_meta"]["spec"]["site_info"]["site_name"]
    # spec.workload_name (device_name) precedes site_info.site_name
    # (workload_name) in SENSITIVE_FIELDS, so its placeholder is the one
    # reused.
    assert anon._PLACEHOLDER_SHAPE["device_name"].match(device_placeholder)
    assert site_placeholder == device_placeholder


def test_real_value_never_persisted(anon: ModuleType) -> None:
    resolved: dict[str, str] = {}
    real = "definitely-real-employee-name"
    data = _workload_config_db({"spec": {"workload_name": real}})

    anon._process_sqlite_bytes(data, resolved, write=False)
    written = anon._process_sqlite_bytes(data, resolved, write=True)

    assert real.encode() not in written


def test_a_deleted_rows_residue_is_scrubbed_from_a_file_with_no_live_match(anon: ModuleType) -> None:
    """A file whose live rows hold no resolved value is still rebuilt when a
    deleted row left one behind in a free page."""
    resolved: dict[str, str] = {}
    real = "definitely-real-device-name"
    anon._process_sqlite_bytes(_workload_config_db({"spec": {"workload_name": real}}), resolved, write=False)
    data = _build_db(
        ["PRAGMA secure_delete = OFF", "CREATE TABLE file_meta (fid INTEGER, path TEXT)"],
        [
            ("INSERT INTO file_meta VALUES (?, ?)", (1, f"/vol1/{real}/disk.img" + "x" * 4000)),
            ("INSERT INTO file_meta VALUES (?, ?)", (2, "/vol1/other/disk.img")),
            ("DELETE FROM file_meta WHERE fid = ?", (1,)),
        ],
    )
    assert real.encode() in data

    written = anon._process_sqlite_bytes(data, resolved, write=True)

    assert real.encode() not in written
    assert _query(written, "SELECT fid, path FROM file_meta") == [(2, "/vol1/other/disk.img")]


def test_idempotent_on_already_anonymized_bytes(anon: ModuleType) -> None:
    resolved: dict[str, str] = {}
    data = _workload_config_db({"spec": {"workload_name": "real-device-01"}})
    anon._process_sqlite_bytes(data, resolved, write=False)
    once = anon._process_sqlite_bytes(data, resolved, write=True)

    resolved_again: dict[str, str] = {}
    anon._process_sqlite_bytes(once, resolved_again, write=False)
    twice = anon._process_sqlite_bytes(once, resolved_again, write=True)

    assert twice == once


def test_vault_link_key_regex_anchors_on_uuid_not_underscore(anon: ModuleType) -> None:
    # The display-name segment itself contains an underscore -- a naive
    # split on "_" would wrongly cut it in two.
    key = "conn123_9053e422-1234-5678-9abc-def012345678_Corp_Backup1"
    match = anon._VAULT_LINK_KEY_RE.match(key)
    assert match is not None
    assert match.group("prefix") == "conn123"
    assert match.group("display") == "Corp_Backup1"


def test_vault_link_key_rewrite_reuses_resolved(anon: ModuleType) -> None:
    resolved = {"Example-Vault": "Test-Workload-arbitrary"}
    key = "conn123_9053e422-1234-5678-9abc-def012345678_Example-Vault"
    new_key = anon._anonymize_vault_link_key(key, resolved)
    assert new_key == "conn123_9053e422-1234-5678-9abc-def012345678_Test-Workload-arbitrary"


def test_anonymize_path_only_rewrites_known_segments(anon: ModuleType) -> None:
    resolved = {"real-device-01": "CORP-PC-arbitrary"}
    assert anon._anonymize_path("/vol/real-device-01/img.bin", resolved) == "/vol/CORP-PC-arbitrary/img.bin"
    assert anon._anonymize_path("/vol/unrelated/img.bin", resolved) == "/vol/unrelated/img.bin"


def test_user_info_persona_is_correlated_across_fields(anon: ModuleType) -> None:
    resolved: dict[str, str] = {}
    user_info = {"name": "Real Person", "email": "realperson@example.com", "user_name": "realperson"}
    changed = anon._anonymize_user_info(user_info, resolved)
    assert changed
    # sha256("realperson@example.com") % 65536 == 0xf57a: all three fields
    # derive from that one slot (``_POOLS``'s "persona" entry).
    assert user_info["name"] == "Anon-f57a"
    assert user_info["user_name"] == "anon-f57a"
    assert user_info["email"] == "anon-f57a@gwsdemo.example.com"


def _fake_ahlt_payload(*, exists: dict[str, bool]) -> dict[str, Any]:
    """A fixture-shaped payload with one aHlT-prefixed read (never
    decrypted: both tests below fail first) and the given root-level
    ``exists()`` markers."""
    return {
        "format": 2,
        "reads": {
            "copy_meta_file/VM_fake/target.db\x000\x00": base64.b64encode(b"aHlT" + b"\x00" * 16).decode("ascii")
        },
        "exists": exists,
        "sizes": {},
        "listdirs": {},
        "missing": {},
    }


async def test_ahlt_payload_raises_a_distinct_error_when_no_layout_is_found(anon: ModuleType) -> None:
    """With no vault/object-store markers, ``iter_repository_layouts()``
    finds nothing and the error names the missing root-level detail, not
    the vault key."""
    payload = _fake_ahlt_payload(exists={})
    with pytest.raises(LookupError, match="root-level detail for iter_repository_layouts"):
        await anon._anonymize_encrypted_payload(payload, {})


async def test_ahlt_payload_raises_a_distinct_error_when_layout_resolves_but_no_key_does(anon: ModuleType) -> None:
    """With a resolvable vault root but no ``db/vault_encryption_key``,
    ``resolve_vault_key()`` returns ``None`` and the error names the vault
    key, not the layout."""
    payload = _fake_ahlt_payload(
        exists={"repo_info": True, "link.key": True, ".fully_created": True, "db/vault_encryption_key": False}
    )
    with pytest.raises(LookupError, match="no known key string could unwrap"):
        await anon._anonymize_encrypted_payload(payload, {})


_SYNTHETIC_VAULT_KEY = bytes(range(32))
_SPEC_VERSION_UID = "00000000-0000-4000-8000-000000000001"


def _encrypted_spec_payload(file_path: str) -> dict[str, Any]:
    """A fixture-shaped payload whose one read is a ``copy_target_version``
    table holding one ``version_spec`` encrypted under ``_SYNTHETIC_VAULT_KEY``."""
    spec = {"status": {"status": "COMPLETED", "file_paths": [file_path]}}
    encrypted = anonymize_catalog_metadata._encrypt_version_spec(spec, _SPEC_VERSION_UID, _SYNTHETIC_VAULT_KEY)
    data = _build_db(
        ["CREATE TABLE copy_target_version (version_uid TEXT, version_spec TEXT)"],
        [("INSERT INTO copy_target_version VALUES (?, ?)", (_SPEC_VERSION_UID, encrypted))],
    )
    return {
        "format": 2,
        "reads": {"db/copy_target_version\x000\x00": base64.b64encode(data).decode("ascii")},
        "exists": {},
        "sizes": {"db/copy_target_version": len(data)},
        "listdirs": {},
        "missing": {},
    }


def _decrypted_specs(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = base64.b64decode(payload["reads"]["db/copy_target_version\x000\x00"])
    return [
        json.loads(decrypt_version_spec(spec, uid, _SYNTHETIC_VAULT_KEY))
        for uid, spec in _query(data, "SELECT version_uid, version_spec FROM copy_target_version")
    ]


async def test_an_encrypted_version_specs_device_name_is_replaced_and_re_encrypted(anon: ModuleType) -> None:
    """The device segment of an encrypted ``version_spec``'s file path gets
    the placeholder a plain one would, and the value stays decryptable."""
    real = "real-device-01"
    payload = _encrypted_spec_payload(f"VM-1/ActiveBackup_2026-01-01_000000/{real}/disk.img")
    anon._VAULT_KEY_CACHE["synthetic"] = _SYNTHETIC_VAULT_KEY

    assert await anon._anonymize_encrypted_payload(payload, {})

    [spec] = _decrypted_specs(payload)
    assert spec["status"]["file_paths"] == [
        f"VM-1/ActiveBackup_2026-01-01_000000/{_REAL_DEVICE_01_PLACEHOLDER}/disk.img"
    ]
    assert spec["status"]["status"] == "COMPLETED"
    raw = base64.b64decode(payload["reads"]["db/copy_target_version\x000\x00"])
    assert payload["sizes"]["db/copy_target_version"] == len(raw)


async def test_a_cached_vault_key_that_does_not_decrypt_the_fixture_is_skipped(anon: ModuleType) -> None:
    payload = _encrypted_spec_payload("VM-1/ActiveBackup_2026-01-01_000000/real-device-01/disk.img")
    anon._VAULT_KEY_CACHE["other-sample"] = bytes(32)
    anon._VAULT_KEY_CACHE["synthetic"] = _SYNTHETIC_VAULT_KEY

    assert await anon._anonymize_encrypted_payload(payload, {})

    [spec] = _decrypted_specs(payload)
    assert spec["status"]["file_paths"][0].split("/")[2] == _REAL_DEVICE_01_PLACEHOLDER


def test_user_info_persona_idempotent(anon: ModuleType) -> None:
    resolved: dict[str, str] = {}
    user_info = {"name": "Real Person", "email": "realperson@example.com", "user_name": "realperson"}
    anon._anonymize_user_info(user_info, resolved)
    before = dict(user_info)

    changed_again = anon._anonymize_user_info(user_info, resolved)

    assert changed_again is False
    assert user_info == before


# -- placeholder minting --


@pytest.mark.parametrize(
    ("category", "preferred", "next_free"),
    [
        ("device_name", "CORP-PC-0000", "CORP-PC-0001"),
        ("ip", "192.0.2.1", "192.0.2.2"),
        ("mac", "02:00:00:00:00:00", "02:00:00:00:00:01"),
    ],
)
def test_a_same_run_collision_probes_forward_to_the_next_free_slot(
    anon: ModuleType, category: str, preferred: str, next_free: str
) -> None:
    digest = "0" * 64
    assert anon._mint_placeholder(category, digest, []) == preferred
    assert anon._mint_placeholder(category, digest, [preferred]) == next_free


def test_a_constant_category_ignores_collisions(anon: ModuleType) -> None:
    assert anon._mint_placeholder("token", "0" * 64, ["deadbeefdeadbeefdeadbeefdeadbeef"]) == (
        "deadbeefdeadbeefdeadbeefdeadbeef"
    )


# -- array-shaped locations --


def test_network_macs_are_replaced_with_locally_administered_placeholders(anon: ModuleType) -> None:
    resolved: dict[str, str] = {}
    blob: dict[str, Any] = {"network": [{"mac_address": "aa:bb:cc:dd:ee:ff"}, {"mac_address": ""}, "not-an-interface"]}

    assert anon._anonymize_network_macs(blob, resolved) is True
    assert re.fullmatch(r"02:00:00:00:[0-9a-f]{2}:[0-9a-f]{2}", blob["network"][0]["mac_address"])
    assert blob["network"][1] == {"mac_address": ""}
    assert anon._anonymize_network_macs(blob, resolved) is False  # already placeholders


def test_a_blob_without_a_network_list_is_untouched(anon: ModuleType) -> None:
    assert anon._anonymize_network_macs({"name": "x"}, {}) is False


def test_json_path_array_rewrites_only_resolved_segments(anon: ModuleType) -> None:
    resolved = {"real-device-01": _REAL_DEVICE_01_PLACEHOLDER}
    blob = {"a": {"files": ["s/real-device-01/f.vmdk", "s/other/f.vmdk", 7]}}

    assert anon._anonymize_json_path_array(blob, "a.files", resolved) is True
    assert blob["a"]["files"] == [f"s/{_REAL_DEVICE_01_PLACEHOLDER}/f.vmdk", "s/other/f.vmdk", 7]
    assert anon._anonymize_json_path_array(blob, "a.missing", resolved) is False


def test_link_dir_names_in_a_listing_are_resolved(anon: ModuleType) -> None:
    resolved: dict[str, str] = {}
    entry = "17_0f3c4e2a-1b2c-4d5e-8f90-123456789abc_Real Team Site"
    anon._extract_link_dir_names({"listdirs": {"db": [entry, "unrelated"]}}, resolved)

    assert set(resolved) == {"Real Team Site"}
    assert resolved["Real Team Site"].startswith("Test-Workload-")


# -- anonymize_fixtures, end to end --


def _write_fixture(path: Path, db: bytes, device: str) -> None:
    db_path = "db/workload_config"
    payload = {
        "format": 2,
        "reads": {f"{db_path}\x000\x00": base64.b64encode(db).decode("ascii")},
        "sizes": {db_path: len(db), f"data/{device}/disk.img": 4096},
        "exists": {f"data/{device}": True},
        "listdirs": {"data": [device], f"data/{device}": ["disk.img"]},
        "missing": {"read": [], "size": [f"data/{device}/meta"], "exists": [], "listdir": []},
    }
    write_fixture_text(path, json.dumps(payload))


def test_anonymize_fixtures_scrubs_catalog_rows_paths_and_listings(anon: ModuleType, tmp_path: Path) -> None:
    fixture = tmp_path / "f.json.gz"
    _write_fixture(fixture, _workload_config_db({"spec": {"workload_name": "real-device-01"}}), "real-device-01")

    assert anon.anonymize_fixtures([fixture]) == [fixture]

    text = load_fixture_text(fixture)
    payload = json.loads(text)
    db = base64.b64decode(payload["reads"]["db/workload_config\x000\x00"])
    assert "real-device-01" not in text
    assert b"real-device-01" not in db
    assert payload["sizes"]["db/workload_config"] == len(db)
    assert payload["listdirs"]["data"] == [_REAL_DEVICE_01_PLACEHOLDER]
    assert f"data/{_REAL_DEVICE_01_PLACEHOLDER}/disk.img" in payload["sizes"]
    assert payload["missing"]["size"] == [f"data/{_REAL_DEVICE_01_PLACEHOLDER}/meta"]


def test_anonymize_fixtures_is_a_no_op_on_its_own_output(anon: ModuleType, tmp_path: Path) -> None:
    fixture = tmp_path / "f.json.gz"
    _write_fixture(fixture, _workload_config_db({"spec": {"workload_name": "real-device-01"}}), "real-device-01")
    anon.anonymize_fixtures([fixture])
    first = load_fixture_text(fixture)

    assert anon.anonymize_fixtures([fixture]) == []
    assert load_fixture_text(fixture) == first


def test_a_value_resolved_in_one_fixture_is_scrubbed_from_another_fixtures_paths(
    anon: ModuleType, tmp_path: Path
) -> None:
    catalog = tmp_path / "catalog.json.gz"
    data_only = tmp_path / "data.json.gz"
    _write_fixture(catalog, _workload_config_db({"spec": {"workload_name": "real-device-01"}}), "x")
    _write_fixture(data_only, _workload_config_db({}), "real-device-01")

    assert anon.anonymize_fixtures([catalog, data_only]) == sorted([catalog, data_only])
    assert "real-device-01" not in load_fixture_text(data_only)
