"""Unit tests for ``scripts/anonymize_catalog_metadata.py``'s structural,
hash-*derived* anonymization engine -- all against synthetic SQLite bytes
built in-test, no real sample data involved.

``scripts/`` isn't an installed package, so the module under test is loaded
by path via ``importlib``, once per test through the ``anon`` fixture --
giving each test a fresh, isolated ``_VAULT_KEY_CACHE`` (the one piece of
module-scope state that actually persists across an invocation; imported
once would leak a resolved vault key from an earlier test into a later
one)."""

from __future__ import annotations

import base64
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "anonymize_catalog_metadata.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("anonymize_catalog_metadata", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered in sys.modules because the module's ``@dataclass``
    # resolves its ``from __future__ import annotations`` string annotations
    # via ``sys.modules[cls.__module__]`` at class-definition time.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def anon() -> ModuleType:
    # A fresh module load already gives a fresh, empty module-level
    # ``_VAULT_KEY_CACHE`` (see the module docstring) -- nothing to reset
    # by hand.
    return _load_module()


def _build_db(ddl: list[str], inserts: list[tuple[str, tuple[Any, ...]]]) -> bytes:
    fd, path_str = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    path = Path(path_str)
    try:
        conn = sqlite3.connect(path)
        try:
            for stmt in ddl:
                conn.execute(stmt)
            for stmt, params in inserts:
                conn.execute(stmt, params)
            conn.commit()
        finally:
            conn.close()
        return path.read_bytes()
    finally:
        path.unlink()


def _query(data: bytes, sql: str) -> list[tuple[Any, ...]]:
    fd, path_str = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    path = Path(path_str)
    try:
        path.write_bytes(data)
        conn = sqlite3.connect(path)
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()
    finally:
        path.unlink()


def _workload_config_db(workload_spec: dict[str, Any]) -> bytes:
    return _build_db(
        ["CREATE TABLE workload_config (workload_id INTEGER, workload_spec TEXT)"],
        [("INSERT INTO workload_config VALUES (?, ?)", (1, json.dumps(workload_spec)))],
    )


#: ``int(sha256("real-device-01").hexdigest(), 16) % 65536``, as 4 hex
#: digits -- computed once here rather than inline in each assertion below,
#: since several tests need the exact same derived placeholder for the
#: exact same real input.
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
    """No shared state is passed between the two calls below at all -- each
    gets its own fresh ``resolved`` *and* there is no module-level
    ``ASSIGNMENTS``-equivalent to leak through either. This is the actual
    property the redesign is for: two genuinely separate invocations
    (different day, different process, different other fixtures in the
    batch) derive the identical placeholder from the real value's hash
    alone, not from anything remembered between them."""
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
    """A real string reused across two differently-categorized fields (here:
    a display name that's also a site name) intentionally gets the *same*
    placeholder for both -- see ``_placeholder_for``'s own docstring for why
    cross-reference correlation (the same real value reads as "the same
    thing" everywhere) wins over each field's placeholder matching its own
    category's shape exactly."""
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
    # "spec.workload_name" (category device_name) precedes "...site_info.site_name"
    # (category workload_name) in SENSITIVE_FIELDS, so it's resolved first -- the
    # site_name field then reuses that same device_name-shaped placeholder verbatim,
    # rather than one of its own category's "Test-Workload-xxxx" shape.
    assert anon._PLACEHOLDER_SHAPE["device_name"].match(device_placeholder)
    assert site_placeholder == device_placeholder


def test_real_value_never_persisted(anon: ModuleType) -> None:
    resolved: dict[str, str] = {}
    real = "definitely-real-employee-name"
    data = _workload_config_db({"spec": {"workload_name": real}})

    anon._process_sqlite_bytes(data, resolved, write=False)
    written = anon._process_sqlite_bytes(data, resolved, write=True)

    assert real.encode() not in written


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
    # sha256("realperson@example.com") % 65536 == 0xf57a -- all three
    # fields derive from that one hash-slot token (see the module
    # docstring's "persona" pool comment), not a fixed name pool.
    assert user_info["name"] == "Anon-f57a"
    assert user_info["user_name"] == "anon-f57a"
    assert user_info["email"] == "anon-f57a@gwsdemo.example.com"


def _fake_ahlt_payload(*, exists: dict[str, bool]) -> dict[str, Any]:
    """A minimal fixture-shaped payload with one aHlT-magic-prefixed read
    (never actually decrypted -- both tests below fail before reaching that
    point) plus whatever root-level ``exists()`` markers the caller wants
    ``iter_layouts()``/``resolve_vault_key()`` to see."""
    return {
        "reads": {
            "copy_meta_file/VM_fake/target.db\x000\x00": base64.b64encode(b"aHlT" + b"\x00" * 16).decode("ascii")
        },
        "exists": exists,
        "sizes": {},
        "listdirs": {},
    }


async def test_ahlt_payload_raises_a_distinct_error_when_no_layout_is_found(anon: ModuleType) -> None:
    """No vault/object-store markers recorded at all -- ``iter_layouts()``
    finds nothing within its own depth limit, so this must fail with the
    "doesn't carry enough root-level detail" message, not the
    vault-key-specific one below."""
    payload = _fake_ahlt_payload(exists={})
    with pytest.raises(LookupError, match="root-level detail for iter_layouts"):
        await anon._anonymize_ahlt_payload(payload, {})


async def test_ahlt_payload_raises_a_distinct_error_when_layout_resolves_but_no_key_does(anon: ModuleType) -> None:
    """A resolvable vault root (all 3 markers present) but no
    ``db/vault_encryption_key`` recorded at all -- ``resolve_vault_key()``
    itself returns ``None`` cleanly (rather than raising), so this must
    fail with the vault-key-specific message, distinct from the
    layout-not-found one above."""
    payload = _fake_ahlt_payload(
        exists={"repo_info": True, "link.key": True, ".fully_created": True, "db/vault_encryption_key": False}
    )
    with pytest.raises(LookupError, match="no known key string could unwrap"):
        await anon._anonymize_ahlt_payload(payload, {})


def test_user_info_persona_idempotent(anon: ModuleType) -> None:
    resolved: dict[str, str] = {}
    user_info = {"name": "Real Person", "email": "realperson@example.com", "user_name": "realperson"}
    anon._anonymize_user_info(user_info, resolved)
    before = dict(user_info)

    changed_again = anon._anonymize_user_info(user_info, resolved)

    assert changed_again is False
    assert user_info == before
