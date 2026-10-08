"""Scrub real names/emails/IPs/tokens out of ``tests/fixtures/*.json.gz``
``RecordingStore`` dumps, replacing each with a deterministic placeholder
(see ``CONTRIBUTING.md``'s "Sample data" section).

No real value and no real-to-fake mapping is stored anywhere: every
placeholder is derived from ``sha256(real value)`` and recognized as
already-a-placeholder by its shape (``_mint_placeholder``,
``_placeholder_for``). ``SENSITIVE_FIELDS`` is structural knowledge -- which
table/column/JSON key path holds customer-derived content -- never a value.

Scope: entries whose raw bytes start with the SQLite file header
(``db/connection_config``, ``workload_config``, ``copy_target_version``,
``file_map``, ``file_meta``, and similar catalog-layer files), plus an
encrypted sample's aHlT-enveloped ``copy_meta_file/<vm>/target.db`` and
encrypted ``copy_target_version.version_spec`` values
(``_anonymize_encrypted_payload``). Dedup-chunked or zstd-enveloped backed-up
content starts with neither header and is never touched. Recorded *paths*
that embed a real value (a workload's display name is also its on-disk
directory name) are rewritten too (``_anonymize_path``).

``SENSITIVE_FIELDS`` is deliberately narrow, with no heuristic guessing, so a
real value at a not-yet-registered location passes through: extend it by
hand, as a table/column/JSON path, when a new real sample surfaces one.

Every run processes the fixtures it is given (default: every
``tests/fixtures/*.json.gz``) through the current registry. Reprocessing an
already-anonymized fixture is a no-op, so re-running after a
``SENSITIVE_FIELDS`` change is how a newly registered field reaches fixtures
recorded earlier.

Usage:
    make anonymize-fixtures                                       # every tests/fixtures/*.json.gz
    make anonymize-fixtures FIXTURES="tests/fixtures/foo.json.gz tests/fixtures/bar.json.gz"

``anonymize_fixtures(paths)`` is the importable form, which
``tests/integration/conftest.py``'s ``pytest_sessionfinish`` calls after a
``--record-against`` session (skipped with ``--no-anonymize``).
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import re
import sqlite3
import tempfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from support.recording.fixture_store import ReplayStore, load_fixture_text, parse_read_key, write_fixture_text
from support.recording.sample_constants import (
    OBJSTORE_ENCRYPTED_KEY_STRING,
    OBJSTORE_M365_ENCRYPTED_KEY_STRING,
    PCPS_ENCRYPTED_KEY_STRING,
    VAULT_ENCRYPTED_KEY_STRING,
)
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.format.compression import ZSTD_FRAME_MAGIC
from synology_apm_repo.sdk.format.crypto import ahlt_decrypt, decrypt_version_spec, version_spec_iv
from synology_apm_repo.sdk.format.headers import HEADER_LEN
from synology_apm_repo.sdk.storage.layout import iter_repository_layouts, key_probe_layout

_AHLT_OFF_IV = 8
_AHLT_IV_LEN = 16


def _ahlt_reencrypt(original: bytes, plaintext: bytes, vault_key: bytes) -> bytes:
    """``plaintext`` AES-256-CTR-encrypted under ``original``'s 64-byte header,
    copied verbatim (IV at ``[8, 24)``); the header encodes no length, so
    ``plaintext`` may differ in length from what it replaced."""
    header = original[:HEADER_LEN]
    iv = header[_AHLT_OFF_IV : _AHLT_OFF_IV + _AHLT_IV_LEN]
    encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(iv)).encryptor()
    return header + encryptor.update(plaintext) + encryptor.finalize()


_SQLITE_MAGIC = b"SQLite format 3\x00"
_AHLT_MAGIC = b"aHlT"

#: An encrypted real sample's ``copy_meta_file/<vm>/target.db`` (aHlT) and
#: ``copy_target_version.version_spec`` values are AES-256-CTR-encrypted, so
#: rewriting them needs that sample's vault key, unwrapped with one of these.
_KNOWN_VAULT_KEY_STRINGS = [
    VAULT_ENCRYPTED_KEY_STRING,
    PCPS_ENCRYPTED_KEY_STRING,
    OBJSTORE_ENCRYPTED_KEY_STRING,
    OBJSTORE_M365_ENCRYPTED_KEY_STRING,
]


# ---------------------------------------------------------------------------
# Structural registry: which fields are customer-derived.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SensitiveField:
    """One customer-derived location: a SQLite ``table``/``column``, and,
    when the column holds JSON rather than being the sensitive scalar
    itself, the ``json_path`` key within it (``None`` for the scalar
    case). ``category`` selects which placeholder pool/shape
    (`_POOLS`/`_is_already_placeholder`) replaces the real value."""

    table: str
    column: str
    json_path: str | None  # None => the column itself is the sensitive scalar
    category: str


SENSITIVE_FIELDS: list[SensitiveField] = [
    SensitiveField("workload_config", "workload_spec", "spec.workload_name", "device_name"),
    SensitiveField("workload_config", "workload_spec", "status.host_name", "device_name"),
    SensitiveField("device_table", "host_name", None, "device_name"),  # copy_meta_file/<vm>/target.db
    SensitiveField("device_table", "other_spec", "name", "device_name"),  # copy_meta_file/<vm>/target.db
    SensitiveField("workload_config", "workload_spec", "status.entity_meta.spec.site_info.site_name", "workload_name"),
    SensitiveField(
        "workload_config", "workload_spec", "status.entity_meta.spec.group_info.display_name", "workload_name"
    ),
    SensitiveField("workload_config", "workload_spec", "status.entity_meta.spec.group_info.mail", "group_email"),
    SensitiveField("workload_config", "workload_spec", "status.entity_meta.spec.team_drive_info.name", "workload_name"),
    SensitiveField("workload_config", "workload_spec", "status.entity_meta.spec.team_info.name", "workload_name"),
    SensitiveField(
        "workload_config", "workload_spec", "status.entity_meta.spec.site_info.owner_id", "site_owner_email"
    ),
    SensitiveField("workload_config", "workload_spec", "status.entity_meta.spec.site_info.url", "sharepoint_url"),
    SensitiveField("workload_config", "workload_spec", "spec.config_fs.host_ip", "ip"),
    SensitiveField("workload_config", "workload_spec", "status.config_pc.private_ip", "ip"),
    SensitiveField("workload_config", "workload_spec", "status.config_pc.public_ip", "ip"),
    SensitiveField("workload_config", "workload_spec", "status.config_ps.private_ip", "ip"),
    SensitiveField("workload_config", "workload_spec", "status.config_ps.public_ip", "ip"),
    SensitiveField("workload_config", "workload_spec", "status.config_pc.agent_token", "token"),
    SensitiveField("workload_config", "workload_spec", "status.config_ps.agent_token", "token"),
    SensitiveField("workload_config", "workload_spec", "spec.config_fs.login_user", "username"),
    # A GWS tenant's domain, stored twice; both get the fixed domain that
    # ``_anonymize_user_info``'s fake addresses also use.
    SensitiveField("workload_config", "workload_spec", "spec.domain", "gws_domain"),
    SensitiveField("workload_config", "workload_spec", "status.entity_meta.spec.domain", "gws_domain"),
]

#: ``status.entity_meta.spec.user_info`` (mailbox display name / email / bare
#: local part) is handled as a unit by ``_anonymize_user_info``, not through
#: ``SENSITIVE_FIELDS``: its three subfields must resolve to one persona.
_USER_INFO_JSON_PATH = "status.entity_meta.spec.user_info"

#: Flat text columns holding a full filesystem path that *embeds* a
#: sensitive value as one segment, rather than being the sensitive value
#: itself -- handled by ``_anonymize_path``'s substring replacement, not the
#: exact-match engine ``SENSITIVE_FIELDS`` drives.
_PATH_COLUMNS: set[tuple[str, str]] = {
    ("file_map", "path"),
    ("file_meta", "path"),
    ("object_table", "file_path"),  # copy_meta_file/<vm>/target.db
    ("object_table", "src_file_path"),
}

#: A column whose entire value is a JSON array of path strings (not nested
#: inside a JSON object the way ``_JSON_PATH_ARRAYS`` entries are), each of
#: which can embed a device's display name as a path segment. A version can
#: outlive its device's catalog row (a renamed or removed device), leaving
#: this array the only place the name appears, so device names are also
#: extracted from it (``_extract_meta_filenames_device_names``).
_JSON_ARRAY_PATH_COLUMNS: set[tuple[str, str]] = {
    ("copy_target_version_meta", "meta_filenames"),
}

#: ``meta_filenames``' own device-bearing shape: ``ActiveBackup_<date>_
#: <time>/<device display name>/<file>`` (a VM's own per-session delta) --
#: as opposed to ``ActiveBackup_<date>_<time>_<uuid>/<file>`` (FS's own
#: session directory, which never has a device-name segment at all).
#: Anchored so a genuine UUID second segment (never a real device name)
#: never gets mistaken for one.
_META_FILENAME_DEVICE_RE = re.compile(r"^ActiveBackup_\d{4}-\d{2}-\d{2}_\d{6}/(?P<device>[^/]+)/[^/]+$")
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


#: ``_PATH_COLUMNS``' values can carry the same device-bearing shape below a
#: VM's top-level session directory (``VM-<uuid>/ActiveBackup_<date>_<time>/
#: <device display name>/<file>``), so this one is searched, not anchored.
_PATH_DEVICE_RE = re.compile(r"ActiveBackup_\d{4}-\d{2}-\d{2}_\d{6}/(?P<device>[^/]+)/")


def _extract_path_device_names(path_str: str, resolved: dict[str, str]) -> None:
    match = _PATH_DEVICE_RE.search(path_str)
    if match is None:
        return
    device = match.group("device")
    if not _UUID_RE.match(device):
        _placeholder_for(device, "device_name", resolved)


def _extract_meta_filenames_device_names(items: list[Any], resolved: dict[str, str]) -> None:
    for item in items:
        if not isinstance(item, str):
            continue
        match = _META_FILENAME_DEVICE_RE.match(item)
        if match is None:
            continue
        device = match.group("device")
        if _UUID_RE.match(device):
            continue
        _placeholder_for(device, "device_name", resolved)


#: JSON blob columns holding an array of path strings (same
#: embeds-a-segment situation as ``_PATH_COLUMNS``, but nested in JSON).
_JSON_PATH_ARRAYS: list[tuple[str, str, str]] = [
    ("copy_target_version", "version_spec", "status.file_paths"),
]

#: ``db/vault_link_key.key`` is ``<connection_id>_<uuid>_<display_name>`` --
#: only the trailing display-name segment is customer-derived; the
#: connection id and UUID are internal identifiers, not content. Anchored on
#: the UUID (rather than splitting on "_") because both the connection id
#: and the display name can themselves contain underscores.
_VAULT_LINK_KEY_RE = re.compile(
    r"^(?P<prefix>.+)_(?P<uuid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
    r"_(?P<display>.+)$"
)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _get_json_path(obj: Any, path: str) -> Any:
    for key in path.split("."):
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def _set_json_path(obj: dict[str, Any], path: str, value: Any) -> None:
    *head, last = path.split(".")
    for key in head:
        obj = obj[key]
    obj[last] = value


# ---------------------------------------------------------------------------
# Placeholder assignment: hash-derived, one-way.
# ---------------------------------------------------------------------------

_FAKE_MAIL_DOMAIN = "gwsdemo.example.com"

#: ``%s`` categories are filled with a 4-hex-char slot derived from the real
#: value's hash (``_mint_placeholder``), so placeholders stay stable without
#: remembered history. ``ip``'s ``%d`` is a hash-derived last octet in the
#: reserved RFC 5737 ``192.0.2.0/24`` block. ``persona``'s slot is the mailbox
#: local part, from which ``_anonymize_user_info`` derives the display name
#: and full address.
_POOLS: dict[str, str] = {
    "device_name": "CORP-PC-%s",
    "workload_name": "Test-Workload-%s",
    "ip": "192.0.2.%d",
    "token": "deadbeefdeadbeefdeadbeefdeadbeef",
    "username": "testuser",
    "group_email": "group-%s@" + _FAKE_MAIL_DOMAIN,
    "gws_domain": _FAKE_MAIL_DOMAIN,
    "site_owner_email": "owner-%s@" + _FAKE_MAIL_DOMAIN,
    "sharepoint_url": "https://contoso.sharepoint.com/sites/site",
    "persona": "anon-%s",
    # A locally-administered OUI ("02" first octet) never collides with a
    # real vendor MAC; the 4-hex slot fills the last two octets.
    "mac": "02:00:00:00:%s",
}

#: Each templated category's placeholder shape, by which
#: ``_is_already_placeholder`` recognizes already-anonymized text. Constant
#: categories (``token``, ``username``, ``gws_domain``, ``sharepoint_url``)
#: are checked by equality instead.
_PLACEHOLDER_SHAPE: dict[str, re.Pattern[str]] = {
    "device_name": re.compile(r"^CORP-PC-[0-9a-f]{4}$"),
    "workload_name": re.compile(r"^Test-Workload-[0-9a-f]{4}$"),
    "ip": re.compile(r"^192\.0\.2\.\d{1,3}$"),
    "group_email": re.compile(r"^group-[0-9a-f]{4}@" + re.escape(_FAKE_MAIL_DOMAIN) + r"$"),
    "site_owner_email": re.compile(r"^owner-[0-9a-f]{4}@" + re.escape(_FAKE_MAIL_DOMAIN) + r"$"),
    "persona": re.compile(r"^anon-[0-9a-f]{4}$"),
    "mac": re.compile(r"^02:00:00:00:[0-9a-f]{2}:[0-9a-f]{2}$"),
}


def _is_already_placeholder(value: str, category: str) -> bool:
    template = _POOLS[category]
    if "%" not in template:
        return value == template
    return bool(_PLACEHOLDER_SHAPE[category].match(value))


def _mint_placeholder(category: str, digest: str, taken: Iterable[str]) -> str:
    """A placeholder derived from ``digest`` (the real value's sha256
    hexdigest). On a collision with ``taken`` (placeholders this invocation
    already handed out) it probes forward to the next free slot, so the
    result depends on which values share the batch."""
    template = _POOLS[category]
    if "%" not in template:
        return template  # a constant placeholder: no per-value uniqueness needed
    taken = set(taken)
    if category == "ip":
        # Usable host octets only: 192.0.2.1-192.0.2.254.
        span = 254
        start = 1 + int(digest, 16) % span
        for step in range(span):
            octet = 1 + (start - 1 + step) % span
            candidate = template % octet
            if candidate not in taken:
                return candidate
        raise LookupError("ip placeholder pool exhausted (192.0.2.1-254)")
    if category == "mac":
        # The generic 4-hex slot, split into two octets.
        span = 1 << 16
        start = int(digest, 16) % span
        for step in range(span):
            slot = (start + step) % span
            hex4 = format(slot, "04x")
            candidate = template % f"{hex4[:2]}:{hex4[2:]}"
            if candidate not in taken:
                return candidate
        raise LookupError("mac placeholder pool exhausted (65536 slots)")
    # A 4-hex-char slot: 65536 possibilities.
    span = 1 << 16
    start = int(digest, 16) % span
    for step in range(span):
        slot = (start + step) % span
        candidate = template % format(slot, "04x")
        if candidate not in taken:
            return candidate
    raise LookupError(f"{category} placeholder pool exhausted (65536 slots)")


def _placeholder_for(real: str, category: str, resolved: dict[str, str]) -> str:
    """The placeholder for ``real``, a function of ``sha256(real)`` alone (up
    to same-batch collisions, see ``_mint_placeholder``), so no mapping is
    stored anywhere.

    ``resolved`` is this invocation's real-to-placeholder map, which
    ``_anonymize_path`` also uses to redact each value wherever it recurs. A
    value already in it keeps its first placeholder even under a different
    ``category``, so one real value has one replacement across every field
    and path segment. A value already shaped like a ``category`` placeholder
    passes through unchanged, which keeps a re-run idempotent."""
    if real in resolved:
        return resolved[real]
    if _is_already_placeholder(real, category):
        resolved[real] = real
        return real
    digest = hashlib.sha256(real.encode("utf-8", errors="surrogateescape")).hexdigest()
    placeholder = _mint_placeholder(category, digest, resolved.values())
    resolved[real] = placeholder
    return placeholder


def _anonymize_user_info(user_info: dict[str, Any], resolved: dict[str, str]) -> bool:
    """Rewrites ``user_info.name``/``.email``/``.user_name`` (display name,
    full address, bare local part) in place as one persona, keyed off one
    identity (``email``, else ``name``, else ``user_name``); returns whether
    anything changed. The local part is a ``persona`` placeholder
    (``anon-<hex>``), the display name its capitalized form, the email it
    plus the fixed fake domain. A ``user_info`` whose fields already have
    that shape is left alone.

    The token is cached under ``"\x00persona\x00" + identity`` rather than
    under ``identity`` itself, because ``identity`` is usually the ``email``
    field, whose own ``resolved`` entry must map to the full fake address
    for ``_anonymize_path``."""
    name, email, user_name = user_info.get("name"), user_info.get("email"), user_info.get("user_name")
    email_local = email.split("@")[0] if email else None
    if (
        (user_name and _is_already_placeholder(user_name, "persona"))
        or (email_local and _is_already_placeholder(email_local, "persona"))
        or (name and _is_already_placeholder(name.lower(), "persona"))
    ):
        return False
    identity = email or name or user_name
    if not identity:
        return False
    cache_key = f"\x00persona\x00{identity}"
    token = resolved.get(cache_key)
    if token is None:
        digest = hashlib.sha256(identity.encode("utf-8", errors="surrogateescape")).hexdigest()
        token = _mint_placeholder("persona", digest, resolved.values())  # e.g. "anon-3f2a"
        resolved[cache_key] = token
    changed = False
    for field, fake in (
        ("name", token.capitalize()),
        ("user_name", token),
        ("email", f"{token}@{_FAKE_MAIL_DOMAIN}" if user_info.get("email") else None),
    ):
        real = user_info.get(field)
        if real and fake and real != fake:
            resolved[real] = fake
            user_info[field] = fake
            changed = True
    return changed


def _anonymize_network_macs(blob: dict[str, Any], resolved: dict[str, str]) -> bool:
    """Rewrites each ``mac_address`` in ``other_spec.network`` (a VM's list of
    network interfaces) -- array elements, which a ``SENSITIVE_FIELDS``
    dotted ``json_path`` can't address. A no-op for a blob without a
    ``network`` list."""
    network = blob.get("network")
    if not isinstance(network, list):
        return False
    changed = False
    for iface in network:
        if not isinstance(iface, dict):
            continue
        mac = iface.get("mac_address")
        if isinstance(mac, str) and mac:
            placeholder = _placeholder_for(mac, "mac", resolved)
            if placeholder != mac:
                iface["mac_address"] = placeholder
                changed = True
    return changed


def _anonymize_json_blob(blob: dict[str, Any], table: str, column: str, resolved: dict[str, str]) -> bool:
    changed = False
    for field in SENSITIVE_FIELDS:
        if field.table != table or field.column != column or field.json_path is None:
            continue
        real = _get_json_path(blob, field.json_path)
        if not isinstance(real, str) or not real:
            continue
        placeholder = _placeholder_for(real, field.category, resolved)
        if placeholder != real:
            _set_json_path(blob, field.json_path, placeholder)
            changed = True
    user_info = _get_json_path(blob, _USER_INFO_JSON_PATH)
    if isinstance(user_info, dict) and _anonymize_user_info(user_info, resolved):
        changed = True
    if _anonymize_network_macs(blob, resolved):
        changed = True
    return changed


def _anonymize_path(path_str: str, resolved: dict[str, str]) -> str:
    """Replace every occurrence of a value already in ``resolved`` -- a
    device/workload's display name is also its on-disk directory name. A
    value no extraction surfaced anywhere in the batch (a registered field,
    ``user_info``, a vault-link key, or a device segment of a recognized
    ``ActiveBackup_*`` path) passes through."""
    for real, fake in sorted(resolved.items(), key=lambda kv: -len(kv[0])):
        if real and real in path_str:
            path_str = path_str.replace(real, fake)
    return path_str


def _anonymize_vault_link_key(key: str, resolved: dict[str, str]) -> str:
    match = _VAULT_LINK_KEY_RE.match(key)
    if match is None:
        return key
    display_name = match.group("display")
    placeholder = _placeholder_for(display_name, "workload_name", resolved)
    if placeholder == display_name:
        return key
    return f"{match.group('prefix')}_{match.group('uuid')}_{placeholder}"


def _anonymize_json_path_array(blob: dict[str, Any], path: str, resolved: dict[str, str]) -> bool:
    values = _get_json_path(blob, path)
    if not isinstance(values, list):
        return False
    changed = False
    new_values = []
    for value in values:
        if isinstance(value, str):
            new_value = _anonymize_path(value, resolved)
            changed = changed or new_value != value
            new_values.append(new_value)
        else:
            new_values.append(value)
    if changed:
        _set_json_path(blob, path, new_values)
    return changed


def _process_column(
    conn: sqlite3.Connection, table: str, column: str, resolved: dict[str, str], *, write: bool
) -> None:
    """Handles one ``(table, column)`` pair by whichever shape it is: a JSON
    blob (``SensitiveField``s with a ``json_path`` and/or a
    ``_JSON_PATH_ARRAYS`` entry), a scalar ``SensitiveField``, the vault-link
    key, a ``_PATH_COLUMNS`` path, or a ``_JSON_ARRAY_PATH_COLUMNS`` array; a
    no-op for any other column. With ``write=False`` it only adds what the
    column supplies to ``resolved``; with ``write=True`` it also rewrites the
    column, ``resolved`` being complete by then.
    """
    json_fields = [f for f in SENSITIVE_FIELDS if f.table == table and f.column == column and f.json_path]
    flat_field = next(
        (f for f in SENSITIVE_FIELDS if f.table == table and f.column == column and f.json_path is None), None
    )
    json_array = next((p for (t, c, p) in _JSON_PATH_ARRAYS if t == table and c == column), None)
    is_vault_link_key = table == "vault_link_key" and column == "key"
    is_path_column = (table, column) in _PATH_COLUMNS
    is_json_array_path_column = (table, column) in _JSON_ARRAY_PATH_COLUMNS
    if not (
        json_fields or flat_field or json_array or is_vault_link_key or is_path_column or is_json_array_path_column
    ):
        return

    col = _quote_ident(column)
    quoted_table = _quote_ident(table)
    rows = conn.execute(f"SELECT rowid, {col} FROM {quoted_table} WHERE {col} IS NOT NULL").fetchall()
    for rowid, value in rows:
        if not isinstance(value, str) or not value:
            continue
        new_value = value
        if json_fields or json_array:
            try:
                blob = json.loads(value)
            except (TypeError, ValueError):
                blob = None
            if isinstance(blob, dict):
                changed_blob = False
                if json_fields:
                    changed_blob = _anonymize_json_blob(blob, table, column, resolved) or changed_blob
                if json_array and write:
                    changed_blob = _anonymize_json_path_array(blob, json_array, resolved) or changed_blob
                if changed_blob:
                    new_value = json.dumps(blob)
        elif flat_field:
            new_value = _placeholder_for(value, flat_field.category, resolved)
        elif is_vault_link_key:
            new_value = _anonymize_vault_link_key(value, resolved)
        elif is_path_column:
            # Extracted on every pass: the path can be the only place a
            # device name appears (see _JSON_ARRAY_PATH_COLUMNS).
            _extract_path_device_names(value, resolved)
            if write:
                new_value = _anonymize_path(value, resolved)
        elif is_json_array_path_column:
            try:
                items = json.loads(value)
            except (TypeError, ValueError):
                items = None
            if isinstance(items, list):
                # Extracted on every pass, as for a path column above.
                _extract_meta_filenames_device_names(items, resolved)
                if write:
                    new_items = [_anonymize_path(i, resolved) if isinstance(i, str) else i for i in items]
                    if new_items != items:
                        new_value = json.dumps(new_items)
        if write and new_value != value:
            conn.execute(f"UPDATE {quoted_table} SET {col} = ? WHERE rowid = ?", (new_value, rowid))


def _holds_real_value(data: bytes, resolved: dict[str, str]) -> bool:
    """Whether any real value ``resolved`` replaces appears in ``data``'s bytes."""
    return any(real.encode("utf-8") in data for real, placeholder in resolved.items() if real and real != placeholder)


def _rewrite_sqlite(data: bytes, edit: Callable[[sqlite3.Connection], bool]) -> bytes:
    """``edit`` run over the SQLite database ``data`` holds; when it returns
    ``True`` the edited database is committed and rebuilt (``VACUUM``, so no
    replaced value survives in a free page) and returned, else ``data``."""
    # A real temp file, not ``deserialize()`` into ``:memory:``: some catalog
    # files carry a WAL-mode header, which the in-memory VFS can't open.
    fd, path = tempfile.mkstemp(suffix=".db")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        conn = sqlite3.connect(path)
        try:
            if not edit(conn):
                return data
            conn.commit()
            conn.execute("VACUUM")
        finally:
            conn.close()
        return Path(path).read_bytes()
    finally:
        os.unlink(path)


def _process_sqlite_bytes(data: bytes, resolved: dict[str, str], *, write: bool) -> bytes:
    """One ``_process_column`` pass over every column of the SQLite database
    ``data`` holds. With ``write=True`` it returns the rewritten database
    when a row changed or ``data`` still holds a resolved real value's bytes
    outside any live row (a deleted row's residue in a free page); otherwise
    ``data`` unchanged. ``data`` that isn't a plain SQLite file -- every
    dedup-chunked or enveloped content read -- is returned untouched."""
    if not data.startswith(_SQLITE_MAGIC):
        return data

    def edit(conn: sqlite3.Connection) -> bool:
        tables = [
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            if not row[0].startswith("sqlite_")
        ]
        changes_before = conn.total_changes
        for table in tables:
            columns = [row[1] for row in conn.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall()]
            for column in columns:
                _process_column(conn, table, column, resolved, write=write)
        return write and (conn.total_changes != changes_before or _holds_real_value(data, resolved))

    return _rewrite_sqlite(data, edit)


def _encrypted_version_specs(data: bytes) -> list[tuple[int, str, str]]:
    """``(rowid, version_uid, version_spec)`` of each ``copy_target_version``
    row in the SQLite database ``data`` holds whose ``version_spec`` is not
    plain JSON -- an encrypted catalog's; empty for any other ``data``."""
    if not data.startswith(_SQLITE_MAGIC):
        return []
    rows: list[tuple[int, str, str]] = []

    def collect(conn: sqlite3.Connection) -> bool:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(copy_target_version)").fetchall()}
        if {"version_uid", "version_spec"} <= columns:
            for rowid, uid, spec in conn.execute("SELECT rowid, version_uid, version_spec FROM copy_target_version"):
                if isinstance(uid, str) and isinstance(spec, str) and spec and not _is_json(spec):
                    rows.append((rowid, uid, spec))
        return False

    _rewrite_sqlite(data, collect)
    return rows


def _is_json(value: str) -> bool:
    try:
        json.loads(value)
    except ValueError:
        return False
    return True


def _decrypt_version_spec_blob(spec: str, version_uid: str, vault_key: bytes) -> dict[str, Any] | None:
    """``spec`` decrypted and JSON-parsed, or ``None`` when ``vault_key``
    doesn't decrypt it to a JSON object."""
    try:
        blob = json.loads(decrypt_version_spec(spec, version_uid, vault_key))
    except (ValueError, UnicodeDecodeError):  # binascii.Error is a ValueError
        return None
    return blob if isinstance(blob, dict) else None


def _encrypt_version_spec(blob: dict[str, Any], version_uid: str, vault_key: bytes) -> str:
    """``decrypt_version_spec``'s inverse: AES-256-CTR under the IV
    ``version_uid`` determines, base64-encoded."""
    encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(version_spec_iv(version_uid))).encryptor()
    ciphertext = encryptor.update(json.dumps(blob).encode("utf-8")) + encryptor.finalize()
    return base64.b64encode(ciphertext).decode("ascii")


def _process_encrypted_version_specs(data: bytes, vault_key: bytes, resolved: dict[str, str]) -> bytes:
    """Rewrite each encrypted ``version_spec`` in the SQLite database
    ``data`` holds as a plain one is rewritten (its ``status.file_paths``,
    see ``_JSON_PATH_ARRAYS``), re-encrypted; each path's device name is
    extracted first, since the plaintext was out of reach until now."""
    rows = _encrypted_version_specs(data)
    if not rows:
        return data

    def edit(conn: sqlite3.Connection) -> bool:
        changed = False
        for rowid, uid, spec in rows:
            blob = _decrypt_version_spec_blob(spec, uid, vault_key)
            if blob is None:
                continue
            paths = _get_json_path(blob, "status.file_paths")
            for path in paths if isinstance(paths, list) else []:
                if isinstance(path, str):
                    _extract_path_device_names(path, resolved)
            if _anonymize_json_path_array(blob, "status.file_paths", resolved):
                new_spec = _encrypt_version_spec(blob, uid, vault_key)
                conn.execute("UPDATE copy_target_version SET version_spec = ? WHERE rowid = ?", (new_spec, rowid))
                changed = True
        return changed or _holds_real_value(data, resolved)

    return _rewrite_sqlite(data, edit)


def _extract_link_dir_names(payload: dict[str, Any], resolved: dict[str, str]) -> None:
    """Resolves the display name of every ``listdirs`` entry, under any
    directory, shaped like a ``db/vault_link_key.key`` value
    (``_VAULT_LINK_KEY_RE``): a fixture can list such a name without ever
    reading ``db/vault_link_key``."""
    for entries in payload.get("listdirs", {}).values():
        for entry in entries:
            match = _VAULT_LINK_KEY_RE.match(entry)
            if match is not None:
                _placeholder_for(match.group("display"), "workload_name", resolved)


def _extract_fixture(payload: dict[str, Any], resolved: dict[str, str]) -> None:
    for b64 in payload["reads"].values():
        _process_sqlite_bytes(base64.b64decode(b64), resolved, write=False)
    _extract_link_dir_names(payload, resolved)


def _replace_read(payload: dict[str, Any], key: str, new_raw: bytes) -> None:
    """Store ``new_raw`` as ``reads[key]``. A catalog SQLite file is recorded
    as one whole-file read (offset 0, no length; ``storage/sqlite.py``'s
    ``open_sqlite``), and a rewrite changes its length, so its ``sizes``
    entry follows."""
    payload["reads"][key] = base64.b64encode(new_raw).decode("ascii")
    file_path, offset, length = parse_read_key(key)
    if offset == 0 and length is None and file_path in payload["sizes"]:
        payload["sizes"][file_path] = len(new_raw)


def _rewrite_fixture(payload: dict[str, Any], resolved: dict[str, str]) -> bool:
    """Rewrite every plain-SQLite ``reads`` entry in ``payload`` and every
    recorded path that embeds a resolved value, in place. ``resolved`` must
    already be complete (see ``_extract_fixture``, run across the whole batch
    first). Returns whether anything changed."""
    changed = False
    for key, b64 in list(payload["reads"].items()):
        raw = base64.b64decode(b64)
        new_raw = _process_sqlite_bytes(raw, resolved, write=True)
        if new_raw != raw:
            changed = True
            _replace_read(payload, key, new_raw)

    for section in ("reads", "sizes", "exists"):
        for old_key in list(payload[section]):
            new_key = _anonymize_path(old_key, resolved)
            if new_key != old_key:
                changed = True
                payload[section][new_key] = payload[section].pop(old_key)
    for old_dir, entries in list(payload["listdirs"].items()):
        new_dir = _anonymize_path(old_dir, resolved)
        new_entries = [_anonymize_path(e, resolved) for e in entries]
        if new_dir != old_dir or new_entries != entries:
            changed = True
            payload["listdirs"].pop(old_dir)
            payload["listdirs"][new_dir] = new_entries
    for method, keys in payload.get("missing", {}).items():
        new_keys = sorted(_anonymize_path(k, resolved) for k in keys)
        if new_keys != sorted(keys):
            changed = True
            payload["missing"][method] = new_keys
    return changed


#: Vault keys already unwrapped, by key string. A vault key belongs to the
#: sample, not one fixture, so one unwrapped from a fixture with enough
#: root-level recording is reused for a sibling fixture that lacks it, once
#: it decrypts that fixture's own encrypted entries.
_VAULT_KEY_CACHE: dict[str, bytes] = {}


async def _harvest_vault_keys(payload: dict[str, Any]) -> bool:
    """Add to ``_VAULT_KEY_CACHE`` every vault key a
    ``_KNOWN_VAULT_KEY_STRINGS`` key unwraps from this fixture's own
    recording; returns whether the recording shows any repository layout."""
    # Not strict: the probe below makes calls the fixture's own tests never did.
    store = ReplayStore(json.dumps(payload), strict=False)
    layouts = []
    try:
        async for layout in iter_repository_layouts(store):
            layouts.append(layout)  # noqa: PERF401 -- keeps what was found before an early end below
    except ApmRepoError:
        pass  # a partial recording ends the walk early
    for layout in layouts:
        for key_string in _KNOWN_VAULT_KEY_STRINGS:
            if key_string in _VAULT_KEY_CACHE:
                continue
            try:
                vault_key = await KeyMaterial.from_key_string(key_string).resolve_vault_key(
                    store, key_probe_layout(layout)
                )
            except ApmRepoError:
                continue  # another sample's key: its key record isn't in this recording
            if vault_key is not None:
                _VAULT_KEY_CACHE[key_string] = vault_key
    return bool(layouts)


async def _vault_key_for(payload: dict[str, Any], fits: Callable[[bytes], bool]) -> bytes:
    """The cached vault key that ``fits`` this fixture's encrypted entries,
    after harvesting this fixture's own (``_harvest_vault_keys``) if none
    does yet. Raises ``LookupError`` when there is none."""
    vault_key = next((key for key in _VAULT_KEY_CACHE.values() if fits(key)), None)
    if vault_key is not None:
        return vault_key
    layout_found = await _harvest_vault_keys(payload)
    vault_key = next((key for key in _VAULT_KEY_CACHE.values() if fits(key)), None)
    if vault_key is not None:
        return vault_key
    if not layout_found:
        raise LookupError(
            "found an encrypted target.db or version_spec, but this fixture's own recording doesn't carry enough "
            "root-level detail for iter_repository_layouts() to find a repository within 2 levels of its root, "
            "and no other fixture in this run supplied a fitting vault key -- rerun with a fixture of the same "
            "sample whose recording includes the exists()/listdir() calls needed to detect its own vault root "
            "(or object-store layout) and a KeyMaterial.resolve_vault_key() probe"
        )
    raise LookupError(
        "found an encrypted target.db or version_spec and resolved this fixture's own layout, but no known key "
        "string could unwrap a wrapped vault key from it -- this fixture's own recording is missing the "
        "db/vault_encryption_key exists()/read() calls resolve_vault_key() needs, and no other fixture in "
        "this run supplied a fitting vault key either -- rerun with a fixture of the same sample whose "
        "recording includes a KeyMaterial.resolve_vault_key() probe"
    )


async def _anonymize_encrypted_payload(payload: dict[str, Any], resolved: dict[str, str]) -> bool:
    """Second pass, after ``_rewrite_fixture``: decrypt, rewrite and
    re-encrypt every encrypted entry -- an aHlT ``reads`` entry (an encrypted
    sample's ``copy_meta_file/<vm>/target.db``) and each encrypted
    ``copy_target_version.version_spec`` -- with the sample's vault key
    (``_vault_key_for``), mutating ``payload`` in place; the caller does the
    file I/O (ASYNC240). Raises ``LookupError`` when no vault key fits. A
    no-op if the fixture has no encrypted entry."""
    reads = {key: base64.b64decode(b64) for key, b64 in payload["reads"].items()}
    ahlt_keys = [key for key, raw in reads.items() if raw.startswith(_AHLT_MAGIC)]
    spec_rows = {key: rows for key, raw in reads.items() if (rows := _encrypted_version_specs(raw))}
    if not ahlt_keys and not spec_rows:
        return False

    def fits(vault_key: bytes) -> bool:
        if spec_rows:
            _, uid, spec = next(iter(spec_rows.values()))[0]
            return _decrypt_version_spec_blob(spec, uid, vault_key) is not None
        # An aHlT file is a SQLite database or a zstd frame (``version.db.zst``).
        return any(
            ahlt_decrypt(reads[key], vault_key).startswith((_SQLITE_MAGIC, ZSTD_FRAME_MAGIC)) for key in ahlt_keys
        )

    vault_key = await _vault_key_for(payload, fits)
    changed = False
    for key in ahlt_keys:
        raw = reads[key]
        plaintext = ahlt_decrypt(raw, vault_key)
        new_plaintext = _process_sqlite_bytes(plaintext, resolved, write=True)
        if new_plaintext != plaintext:
            changed = True
            _replace_read(payload, key, _ahlt_reencrypt(raw, new_plaintext, vault_key))
    for key in spec_rows:
        raw = reads[key]
        new_raw = _process_encrypted_version_specs(raw, vault_key, resolved)
        if new_raw != raw:
            changed = True
            _replace_read(payload, key, new_raw)
    return changed


async def _anonymize_all_encrypted(payloads: dict[Path, dict[str, Any]], resolved: dict[str, str]) -> list[Path]:
    """``_anonymize_encrypted_payload`` over every payload in place,
    returning the paths it changed; the caller does the file I/O (ASYNC240).
    A fixture that raised ``LookupError`` is retried once after the rest,
    with every fixture's vault keys harvested into ``_VAULT_KEY_CACHE``."""
    changed_paths: list[Path] = []
    deferred: list[Path] = []
    for path, payload in payloads.items():
        try:
            if await _anonymize_encrypted_payload(payload, resolved):
                changed_paths.append(path)
        except LookupError:
            deferred.append(path)
    if deferred:
        # A sibling fixture with no encrypted entry of its own may still
        # carry its sample's key probe.
        for payload in payloads.values():
            await _harvest_vault_keys(payload)
    for path in deferred:
        changed = await _anonymize_encrypted_payload(payloads[path], resolved)
        changed_paths += [path] if changed else []
    return changed_paths


def anonymize_fixtures(fixtures: list[Path]) -> list[Path]:
    """Anonymize ``fixtures`` as one batch sharing one ``resolved`` map, and
    write back whichever changed; returns the paths rewritten."""
    payloads = {path: json.loads(load_fixture_text(path)) for path in fixtures}

    # Extract across the whole batch first: a value can appear as a catalog
    # field in one fixture and only as a path segment in another.
    resolved: dict[str, str] = {}
    for payload in payloads.values():
        _extract_fixture(payload, resolved)

    changed_paths: list[Path] = []
    for path, payload in payloads.items():
        if _rewrite_fixture(payload, resolved):
            changed_paths.append(path)
    for path in changed_paths:
        write_fixture_text(path, json.dumps(payloads[path], indent=1, sort_keys=True))

    encrypted_changed_paths = asyncio.run(_anonymize_all_encrypted(payloads, resolved))
    for path in encrypted_changed_paths:
        write_fixture_text(path, json.dumps(payloads[path], indent=1, sort_keys=True))

    return sorted(set(changed_paths) | set(encrypted_changed_paths))


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "fixtures",
        nargs="*",
        type=Path,
        help="fixture paths to anonymize (default: every tests/fixtures/*.json.gz)",
    )
    args = parser.parse_args()
    fixtures = args.fixtures or sorted(Path("tests/fixtures").glob("*.json.gz"))

    for path in anonymize_fixtures(fixtures):
        print(f"anonymized {path}")


if __name__ == "__main__":
    _main()
