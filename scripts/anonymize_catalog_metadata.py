"""Scrub real names/emails/IPs/tokens out of ``tests/fixtures/*.json.gz``
``RecordingStore`` dumps' plain-SQLite entries, replacing each with a
deterministic placeholder -- this repository's standardized fake-data
convention (see ``CONTRIBUTING.md``'s "Sample data" section).

No real value is ever written anywhere, and no external state is kept to
make placeholders stable: every placeholder is derived purely from
``sha256(real value)``, and detected as already-a-placeholder by shape,
not by remembered history -- see ``_mint_placeholder`` for the
derivation/collision-probing mechanism and ``_placeholder_for`` for the
real-to-digest mapping and idempotency check. ``SENSITIVE_FIELDS`` below is
*structural* knowledge (which table/column/JSON-key-path holds
customer-derived content) rather than a list of real values.

Scope: entries whose raw bytes start with the SQLite file header
(``db/connection_config``, ``workload_config``, ``copy_target_version``,
``file_map``, ``file_meta``, and similar catalog-layer files), plus the one
aHlT-encrypted exception an encrypted real sample's own
``copy_meta_file/<vm>/target.db`` needs (see ``_anonymize_ahlt_payload``). A
dedup-chunked or ZSTD-enveloped read of actual backed-up content never
starts with either header, so this never touches it -- see
``CONTRIBUTING.md`` for why that layer is a separate, larger effort not
covered here. Recorded *paths* that embed a real value (a workload's
display name is also its on-disk directory name) are rewritten too --
see ``_anonymize_path``.

``SENSITIVE_FIELDS`` is deliberately narrow: nothing here heuristically
guesses which fields are customer-derived, so a real value at a
not-yet-registered location silently passes through. Extend the list by
hand -- as a table/column/JSON-path, never as a value -- when a new real
sample surfaces one.

Every invocation processes exactly the fixtures it's given (default: every
``tests/fixtures/*.json.gz``) through the current registry, full stop --
there is no permanently-exempt fixture list and no "was this fixture
already handled" tracking to keep in sync by hand. That's safe on every
commit, not just the first time a fixture is recorded: reprocessing an
already-anonymized fixture is a true no-op (see ``_placeholder_for``), so
running this after any change to ``SENSITIVE_FIELDS`` is exactly how a
newly-registered field's placeholder reaches fixtures recorded before that
field existed.

Usage:
    uv run python scripts/anonymize_catalog_metadata.py                    # every tests/fixtures/*.json.gz
    uv run python scripts/anonymize_catalog_metadata.py tests/fixtures/foo.json.gz tests/fixtures/bar.json.gz

``anonymize_fixtures(paths)`` is the same batch logic as an importable
function -- ``tests/conftest.py``'s own post-recording hook calls it
directly (rather than shelling out to this file) so recording a fixture
via ``pytest --record-against=...`` anonymizes it automatically at session
end; see that fixture's own docstring and its ``--no-anonymize`` opt-out.
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
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.format.crypto import ahlt_decrypt
from synology_apm_repo.sdk.format.headers import HEADER_LEN
from synology_apm_repo.sdk.storage.layout import iter_layouts
from synology_apm_repo.sdk.storage.recording import ReplayStore, load_fixture_text, write_fixture_text

_AHLT_OFF_IV = 8
_AHLT_IV_LEN = 16


def _ahlt_reencrypt(original: bytes, plaintext: bytes, vault_key: bytes) -> bytes:
    """The header (including its IV) is copied verbatim from ``original`` --
    AES-256-CTR is its own inverse given the same key/IV, so encrypting
    ``plaintext`` with that same IV reproduces a validly-enveloped file, even
    when ``plaintext``'s length differs from what the header originally wrapped
    (see ``ahlt_decrypt``'s own docstring for the envelope shape; nothing in
    the header encodes a length that would need adjusting to match)."""
    header = original[:HEADER_LEN]
    iv = header[_AHLT_OFF_IV : _AHLT_OFF_IV + _AHLT_IV_LEN]
    encryptor = Cipher(algorithms.AES(vault_key), modes.CTR(iv)).encryptor()
    return header + encryptor.update(plaintext) + encryptor.finalize()


_SQLITE_MAGIC = b"SQLite format 3\x00"
_AHLT_MAGIC = b"aHlT"

#: ``copy_meta_file/<vm>/target.db`` is aHlT-encrypted (AES-256-CTR) in an
#: encrypted real sample, rather than the plain SQLite bytes every other
#: catalog file uses -- unwrapping it needs that sample's own vault key,
#: not just a byte match. This project's tests already commit each
#: encrypted sample's own key string (it unlocks that sample's own
#: synthetic vault, not customer data -- see tests/CLAUDE.md's
#: "Recording a fixture" section), so reuse those here to reach the same
#: workload names that live in every other (plain) catalog file.
_KNOWN_VAULT_KEY_STRINGS = [
    "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM=",
]


# ---------------------------------------------------------------------------
# Structural registry: WHICH fields are customer-derived. Safe to commit --
# this is knowledge about the catalog schema, never a real value. Add an
# entry (table, column, JSON key path within it, category) when a new real
# sample surfaces a field not yet listed; see the module docstring's
# "SENSITIVE_FIELDS is deliberately narrow" paragraph for why nothing here
# tries to infer this automatically.
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
    # A GWS tenant's own domain, present as two independent copies of the
    # same value (a top-level ``spec.domain`` and a nested
    # ``status.entity_meta.spec.domain``) -- both feed ``Workload.domain``
    # (see catalog.py's own docstring), so both need the same fixed
    # placeholder as ``user_info.email``'s own domain suffix (below).
    SensitiveField("workload_config", "workload_spec", "spec.domain", "gws_domain"),
    SensitiveField("workload_config", "workload_spec", "status.entity_meta.spec.domain", "gws_domain"),
]

#: ``status.entity_meta.spec.user_info`` (mailbox display name / email / bare
#: local part) is handled as a unit by ``_anonymize_user_info``, not through
#: ``SENSITIVE_FIELDS`` -- its three subfields must resolve to the *same*
#: persona (e.g. a real display name / email / bare local part all -> the
#: same "alice" persona) rather than three independently hashed,
#: uncorrelated placeholders.
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
#: inside a JSON object the way ``_JSON_PATH_ARRAYS`` entries are) --
#: ``copy_target_version_meta.meta_filenames`` embeds a device's display
#: name as a path segment the same way ``_PATH_COLUMNS``' plain string
#: columns do, just JSON-array-encoded. Unlike ``_PATH_COLUMNS``, this one
#: gets its own extraction (below) rather than only consuming a value
#: ``workload_config``/``device_table`` resolved elsewhere: a version this
#: old can outlive the device's own current catalog row entirely (a
#: renamed or removed device no longer has a matching ``workload_config``
#: entry at all), leaving this array as the only surviving place its name
#: appears.
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


#: ``_PATH_COLUMNS``' own values (``file_map``/``file_meta``/
#: ``object_table`` paths) can carry the same device-bearing shape one
#: level deeper, prefixed by the VM's own top-level session directory
#: (e.g. ``VM-<uuid>/ActiveBackup_<date>_<time>/<device display name>/
#: <file>``) -- searched rather than fully matched, unlike
#: ``_META_FILENAME_DEVICE_RE``, since the device segment isn't
#: necessarily the path's first component here.
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
# Placeholder assignment: one-way, hash-*derived* -- no external file, no
# in-process memory that outlives one invocation. See the module docstring's
# "no external state is kept" paragraph for why this is safe and stable.
# ---------------------------------------------------------------------------

_FAKE_MAIL_DOMAIN = "gwsdemo.example.com"

#: ``%s`` categories are filled with a 4-hex-char slot derived from the real
#: value's own hash (see ``_mint_placeholder``) -- not a sequential counter,
#: so no history needs remembering to keep them stable. ``ip`` keeps ``%d``
#: (a decimal last octet, computed the same hash-derived way -- see
#: ``_mint_placeholder``'s own branch for it) to stay a plausible-looking
#: address in the reserved RFC 5737 ``192.0.2.0/24`` block. ``persona``'s
#: own slot is the mailbox local part; ``_anonymize_user_info`` derives the
#: display name and full address from that same slot rather than minting
#: them separately -- see its own docstring for why a fixed name pool
#: (Alice/Bob/...) isn't needed: nothing downstream cares whether a mailbox
#: owner's fake name looks like a human name, only that it's a string and,
#: for the email field, that it has a valid `local@domain` shape.
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
    # A locally-administered OUI (the "02" first octet's second-least-
    # significant bit set) never collides with a real vendor MAC -- see
    # _mint_placeholder's own "mac" branch for how the trailing 4-hex slot
    # is split into the last two octets.
    "mac": "02:00:00:00:%s",
}

#: Matches a category's own placeholder *shape* -- used by
#: ``_is_already_placeholder`` to recognize already-anonymized text
#: statelessly (see the module docstring). Only categories with a ``%s``
#: slot need one; ``ip``'s shape is simple enough to check without a
#: precompiled pattern (any address in the reserved 192.0.2.0/24 block was
#: never real customer data to begin with), and ``token``/``username`` are
#: plain constants checked by equality.
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
    """Derive a placeholder purely from ``digest`` (``real``'s own sha256
    hexdigest) -- no counter, no persisted history. ``taken`` (the
    placeholders this same invocation has already handed out -- see
    ``_placeholder_for``'s ``resolved.values()``) is only consulted to probe
    forward to the next free slot on a same-run collision between two
    genuinely different real values; it never needs remembering beyond one
    invocation, since the *next* invocation recomputes the same preferred
    slot for the same values in the same (already-stable) order and gets
    the same collision, resolved the same way."""
    template = _POOLS[category]
    if "%" not in template:
        return template  # constant placeholder (token, username): no per-value uniqueness needed
    taken = set(taken)
    if category == "ip":
        # Keep the last octet in the reserved block's usable range
        # (192.0.2.1-192.0.2.254 -- .0/.255 are the network/broadcast
        # addresses even in a /24 nobody routes).
        span = 254
        start = 1 + int(digest, 16) % span
        for step in range(span):
            octet = 1 + (start - 1 + step) % span
            candidate = template % octet
            if candidate not in taken:
                return candidate
        raise LookupError("ip placeholder pool exhausted (192.0.2.1-254)")
    if category == "mac":
        # Same 4-hex-char slot as the generic branch below, just split into
        # two colon-joined octets so the result reads as a MAC address
        # rather than one unbroken hex run.
        span = 1 << 16
        start = int(digest, 16) % span
        for step in range(span):
            slot = (start + step) % span
            hex4 = format(slot, "04x")
            candidate = template % f"{hex4[:2]}:{hex4[2:]}"
            if candidate not in taken:
                return candidate
        raise LookupError("mac placeholder pool exhausted (65536 slots)")
    # A 4-hex-char slot: 65536 possibilities, ample for this project's real
    # sample corpus -- see the module docstring for the collision-probing
    # contract.
    span = 1 << 16
    start = int(digest, 16) % span
    for step in range(span):
        slot = (start + step) % span
        candidate = template % format(slot, "04x")
        if candidate not in taken:
            return candidate
    raise LookupError(f"{category} placeholder pool exhausted (65536 slots)")


def _placeholder_for(real: str, category: str, resolved: dict[str, str]) -> str:
    """Deterministic, one-way real -> placeholder mapping: the same ``real``
    always yields the same placeholder, purely as a function of
    ``sha256(real)`` -- no external state, so separate invocations (any
    machine, any day, any other fixtures in the batch) agree without needing
    to remember anything. ``resolved`` is this one invocation's own cache
    (also what ``_anonymize_path``/``_anonymize_vault_link_key`` consult to
    redact a value wherever it recurs, including inside paths, and what
    ``_mint_placeholder`` probes against to resolve a same-run collision).

    ``real`` already matching its category's placeholder shape (re-running
    against an already-anonymized fixture) is passed through unchanged
    rather than hashed and minted again -- otherwise a second run would
    treat its own prior output as new real content and rename it, breaking
    idempotency.

    A real value that legitimately recurs under a *different* category (a
    workload's ``host_ip`` happening to hold the same string as its
    ``host_name``, say) intentionally gets back the *same* placeholder as
    its first occurrence rather than a category-correct one of its own:
    ``resolved`` correlates a real value to one output everywhere it
    appears, across every field and path segment (see
    ``_anonymize_vault_link_key``'s own call and ``_anonymize_path``'s
    docstring) -- a real value cross-referenced by both a catalog field and
    a directory path only stays legible as "the same thing" if both get the
    identical replacement, which matters more here than any one field's
    placeholder matching its own category's shape exactly. The (rare) cost
    is cosmetic, not a disclosure risk: the second field renders with the
    wrong category's shape, but the real value is still fully replaced
    either way."""
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
    """``user_info.name``/``.email``/``.user_name`` (mailbox display name, full
    address, bare local part) must resolve to the same persona wherever
    they recur, so this is keyed off one identity (the email if present,
    else the display name) rather than three independently hashed fields.
    Mutates ``user_info`` in place; returns whether anything changed.

    Derives all three fields from one ``persona``-category placeholder
    (see ``_placeholder_for`` -- same hash-slot-with-collision-probing
    mechanism ``device_name``/``workload_name`` already use, not a
    separately-maintained fixed name pool): the mailbox local part *is*
    the placeholder itself (``anon-<hex>``), the display name is its
    capitalized form, and the email address appends the fixed fake
    domain. Nothing downstream needs a human-sounding name here, only a
    correctly-shaped `local@domain` string for the email field -- see the
    module docstring's ``persona`` pool comment.

    Already-assigned fields (re-running against an already-anonymized
    fixture) are left alone rather than re-hashed -- checked via the same
    shape match ``_is_already_placeholder`` uses for every other category,
    against whichever of the three fields is present.

    Mints the token via a namespaced cache key inside ``resolved``
    (``"\\x00persona\\x00" + identity``), deliberately *not*
    ``_placeholder_for(identity, "persona", resolved)`` directly: identity
    is usually the same string as one of the three fields below (typically
    ``email``), and that field's own entry in ``resolved`` needs to map to
    its *full* fake value (``anon-<hex>@domain``, for ``_anonymize_path``'s
    later substitution use) rather than the bare token -- sharing one key
    for both would make each overwrite the other, corrupting whichever a
    later call reads back first."""
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
    """``other_spec.network`` (a VM hardware-spec blob's list of
    network-interface objects) embeds each interface's own MAC address --
    an array element, which ``SENSITIVE_FIELDS``' dotted ``json_path``
    can't address, so this gets its own small function the same way
    ``_anonymize_user_info`` does. A no-op for any blob without a
    ``network`` list (every non-VM catalog blob)."""
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
    """Replace every path segment matching a value already resolved
    elsewhere this run (see ``_placeholder_for``) -- a device/workload's
    display name is also its on-disk directory name, so any path
    embedding one needs the same substitution. Only catches values a
    ``SENSITIVE_FIELDS``/``user_info``/``vault_link_key`` field surfaced
    somewhere in the batch; a value that appeared *only* as a path
    segment, nowhere else in the catalog, isn't something this script can
    reach at all."""
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
    """Handles one ``(table, column)`` pair, dispatching to whichever of the
    five mutually-exclusive shapes it matches (a ``SensitiveField`` with a
    ``json_path`` or a bare JSON-array column -- the two share one branch,
    since both rewrite a JSON blob -- a ``SensitiveField`` without a
    ``json_path``, the vault-link key, a path column, or a
    JSON-array-of-paths column) -- a no-op if none match.
    Runs twice per column across a session: once with ``write=False`` to
    extract every device/category name this column can supply into
    ``resolved`` (some columns are the *only* surviving place a name
    appears, so every column must be scanned before any column is
    rewritten), then again with ``write=True`` to actually rewrite values
    using the now-complete ``resolved`` map.
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
            # Extraction runs every pass (write=False included) -- same
            # reason as _JSON_ARRAY_PATH_COLUMNS below: a version this old
            # can outlive the device's own current catalog row entirely,
            # leaving this path the only surviving place its name appears.
            _extract_path_device_names(value, resolved)
            if write:
                new_value = _anonymize_path(value, resolved)
        elif is_json_array_path_column:
            try:
                items = json.loads(value)
            except (TypeError, ValueError):
                items = None
            if isinstance(items, list):
                # Extraction runs every pass (write=False included) -- this
                # column can be the *only* surviving place a device name
                # appears (see the field's own comment), so resolved must
                # learn it here rather than assume some other field already
                # will.
                _extract_meta_filenames_device_names(items, resolved)
                if write:
                    new_items = [_anonymize_path(i, resolved) if isinstance(i, str) else i for i in items]
                    if new_items != items:
                        new_value = json.dumps(new_items)
        if write and new_value != value:
            conn.execute(f"UPDATE {quoted_table} SET {col} = ? WHERE rowid = ?", (new_value, rowid))


def _process_sqlite_bytes(data: bytes, resolved: dict[str, str], *, write: bool) -> bytes:
    """One pass over every registered table/column of the SQLite database
    ``data`` holds. In extract mode (``write=False``) this only resolves
    exact-value sensitive fields into ``resolved``
    -- ``data`` is returned unchanged. In write mode (``write=True``) it also
    rewrites those same fields plus every path-embedding column/JSON array,
    using ``resolved`` (assumed complete by then -- see ``_main``'s two-phase
    batch structure: extract every fixture first, then rewrite all of
    them). A no-op if ``data`` isn't a plain SQLite file -- a dedup-chunked
    or enveloped content read never starts with the SQLite header, so this
    is what keeps that layer untouched without the caller needing to know
    which path it came from."""
    if not data.startswith(_SQLITE_MAGIC):
        return data
    # A real temp file, not ``sqlite3.Connection.deserialize()`` into
    # ``:memory:`` -- some of these catalog files carry a WAL-mode header,
    # and SQLite's in-memory VFS can't satisfy WAL mode's own file-backing
    # requirement, failing every query with ``unable to open database file``.
    fd, path = tempfile.mkstemp(suffix=".db")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        conn = sqlite3.connect(path)
        try:
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
            if not write or conn.total_changes == changes_before:
                return data
            conn.commit()
            # A plain UPDATE doesn't zero out the old bytes it replaces --
            # SQLite just marks the old page content free, so the real
            # value stays physically present in freelist/overflow slack
            # space even though every query now returns the placeholder.
            # VACUUM rebuilds the file from scratch, leaving nothing but
            # the current (already-anonymized) row values on disk.
            conn.execute("VACUUM")
        finally:
            conn.close()
        return Path(path).read_bytes()
    finally:
        os.unlink(path)


def _extract_link_dir_names(payload: dict[str, Any], resolved: dict[str, str]) -> None:
    """``listdirs`` entries shaped like ``db/vault_link_key.key``
    (``<connection_id>_<uuid>_<display_name>``, see ``_VAULT_LINK_KEY_RE``)
    can surface directly in a recorded directory listing -- not just as a
    SQLite column value -- when a fixture never happened to read
    ``db/vault_link_key`` itself. Scans every ``listdirs`` entry regardless
    of which directory it was listed under (the shape itself is the
    signal, not any one specific path) so ``resolved`` learns the display
    name before ``_rewrite_fixture``'s generic ``_anonymize_path``
    substitution runs over the same ``listdirs`` data."""
    for entries in payload.get("listdirs", {}).values():
        for entry in entries:
            match = _VAULT_LINK_KEY_RE.match(entry)
            if match is not None:
                _placeholder_for(match.group("display"), "workload_name", resolved)


def _extract_fixture(payload: dict[str, Any], resolved: dict[str, str]) -> None:
    for b64 in payload["reads"].values():
        _process_sqlite_bytes(base64.b64decode(b64), resolved, write=False)
    _extract_link_dir_names(payload, resolved)


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
            payload["reads"][key] = base64.b64encode(new_raw).decode("ascii")
        # ``key`` is "path\x00offset\x00length" (storage/recording.py's
        # _read_key). A whole-file read (offset 0, no length) is the only
        # shape a raw catalog SQLite file is ever recorded at -- see
        # storage/sqlite.py's ``open_sqlite`` -- and its ``sizes`` entry needs
        # the same new length, since SQLite page reallocation on UPDATE
        # can change file size even when every replacement string is a
        # different length than what it replaced.
        file_path, offset, length = key.split("\x00")
        if offset == "0" and length == "" and file_path in payload["sizes"]:
            payload["sizes"][file_path] = len(new_raw)

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
    return changed


#: Populated as fixtures resolve successfully -- a vault key is a property
#: of the real sample, not of any one fixture, so once GCM-unwrapped
#: (verified correct by construction: a wrong key/nonce pair fails that
#: check outright) from whichever fixture happens to carry enough
#: root-level recording for ``iter_layouts`` to find a repository, it's reused
#: as-is for any other fixture recorded from the same sample that
#: doesn't carry that same root-level detail.
_VAULT_KEY_CACHE: dict[str, bytes] = {}


async def _resolve_known_vault_key(store: ReplayStore, layout: object) -> bytes | None:
    for key_string in _KNOWN_VAULT_KEY_STRINGS:
        if key_string in _VAULT_KEY_CACHE:
            return _VAULT_KEY_CACHE[key_string]
        vault_key = await KeyMaterial.from_key_string(key_string).resolve_vault_key(store, layout)  # type: ignore[arg-type]
        if vault_key is not None:
            _VAULT_KEY_CACHE[key_string] = vault_key
            return vault_key
    return None


async def _anonymize_ahlt_payload(payload: dict[str, Any], resolved: dict[str, str]) -> bool:
    """Second pass, after ``_rewrite_fixture``: unwrap and rewrite any
    ``copy_meta_file/<vm>/target.db`` entry that's aHlT-encrypted rather
    than plain SQLite (an encrypted real sample's own VM metadata) -- see
    ``_KNOWN_VAULT_KEY_STRINGS``'s own comment for why this is safe to do
    with a committed key string. ``payload`` (an already-parsed fixture
    dict) is mutated in place; the caller owns reading/writing the file --
    kept out of this ``async def`` since both are blocking calls (ASYNC240).
    A no-op if the fixture has no aHlT entry."""
    ahlt_keys = [k for k, b64 in payload["reads"].items() if base64.b64decode(b64).startswith(_AHLT_MAGIC)]
    if not ahlt_keys:
        return False

    store = ReplayStore(json.dumps(payload))
    vault_key = _VAULT_KEY_CACHE.get(_KNOWN_VAULT_KEY_STRINGS[0]) if _KNOWN_VAULT_KEY_STRINGS else None
    layout_found = False
    if vault_key is None:
        try:
            layout = await anext(iter_layouts(store), None)
        except Exception:
            layout = None
        if layout is not None:
            layout_found = True
            vault_key = await _resolve_known_vault_key(store, layout)
    if vault_key is None:
        if not layout_found:
            raise LookupError(
                "found an aHlT-encrypted target.db, but this fixture's own recording doesn't carry enough "
                "root-level detail for iter_layouts() to find a repository within 2 levels of its root, and no other "
                "fixture in this run resolved a usable vault key first -- rerun with a fixture whose recording "
                "includes the exists()/listdir() calls needed to detect its own vault root (or object-store "
                "layout), processed earlier in the same invocation"
            )
        raise LookupError(
            "found an aHlT-encrypted target.db and resolved this fixture's own layout, but no known key string "
            "could unwrap a wrapped vault key from it -- this fixture's own recording is missing the "
            "db/vault_encryption_key exists()/read() calls resolve_vault_key() needs, and no other fixture in "
            "this run resolved a usable vault key first either -- rerun with a fixture whose recording includes "
            "a KeyMaterial.resolve_vault_key() probe, processed earlier in the same invocation"
        )

    changed = False
    for key in ahlt_keys:
        raw = base64.b64decode(payload["reads"][key])
        plaintext = ahlt_decrypt(raw, vault_key)
        new_plaintext = _process_sqlite_bytes(plaintext, resolved, write=True)
        if new_plaintext == plaintext:
            continue
        changed = True
        new_raw = _ahlt_reencrypt(raw, new_plaintext, vault_key)
        payload["reads"][key] = base64.b64encode(new_raw).decode("ascii")
        file_path, offset, length = key.split("\x00")
        if offset == "0" and length == "" and file_path in payload["sizes"]:
            payload["sizes"][file_path] = len(new_raw)
    return changed


async def _anonymize_all_ahlt(payloads: dict[Path, dict[str, Any]], resolved: dict[str, str]) -> list[Path]:
    """Mutates each already-loaded ``payloads[path]`` in place (see
    ``_anonymize_ahlt_payload``); the caller owns all file I/O (ASYNC240) --
    reading ``payloads`` in beforehand and writing back whichever paths this
    returns as changed. Two passes for the same reason
    ``_anonymize_ahlt_payload`` can raise ``LookupError``: a fixture whose own
    recording lacks enough root-level detail for ``iter_layouts()`` to find a
    repository (or lacks the ``db/vault_encryption_key`` probe ``resolve_vault_key()``
    needs) can only succeed once some *other* fixture from the same sample has
    already populated ``_VAULT_KEY_CACHE`` -- which one runs first depends
    purely on dict/glob order, so a single pass can hit one before its
    cache-donor."""
    changed_paths: list[Path] = []
    deferred: list[Path] = []
    for path, payload in payloads.items():
        try:
            if await _anonymize_ahlt_payload(payload, resolved):
                changed_paths.append(path)
        except LookupError:
            deferred.append(path)
    for path in deferred:
        changed = await _anonymize_ahlt_payload(payloads[path], resolved)
        changed_paths += [path] if changed else []
    return changed_paths


def anonymize_fixtures(fixtures: list[Path]) -> list[Path]:
    """Anonymize exactly the given fixtures as one batch (shared ``resolved``
    map across all of them -- see the module docstring's two-phase
    extract/rewrite description) and write back whichever ones changed.
    Returns the paths actually rewritten. The single entry point both
    ``_main()`` (the standalone CLI) and ``tests/conftest.py``'s own
    post-recording hook call -- see that fixture's docstring for why
    recording a fixture runs this automatically rather than leaving it a
    separate manual step."""
    payloads = {path: json.loads(load_fixture_text(path)) for path in fixtures}

    # Phase 1: extract every registered field's current value across the
    # *whole* batch first, so phase 2's path substitution can catch a value
    # that only appears structurally in one fixture but as a path segment
    # in another.
    resolved: dict[str, str] = {}
    for payload in payloads.values():
        _extract_fixture(payload, resolved)

    changed_paths: list[Path] = []
    for path, payload in payloads.items():
        if _rewrite_fixture(payload, resolved):
            changed_paths.append(path)
    for path in changed_paths:
        write_fixture_text(path, json.dumps(payloads[path], indent=1, sort_keys=True))

    ahlt_changed_paths = asyncio.run(_anonymize_all_ahlt(payloads, resolved))
    for path in ahlt_changed_paths:
        write_fixture_text(path, json.dumps(payloads[path], indent=1, sort_keys=True))

    return sorted(set(changed_paths) | set(ahlt_changed_paths))


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
