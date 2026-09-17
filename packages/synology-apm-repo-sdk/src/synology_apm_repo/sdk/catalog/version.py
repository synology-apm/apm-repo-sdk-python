"""``Version``: one ``db/copy_target_version`` row (+ ``_meta`` when
present), its own meta-availability checks, and ``open_target_db()`` — the
one place this layer does real decrypt work (shared by the Unit Layer's
Device and FS providers). Display/status extraction
(``_version_epoch``/``_version_display_name``) is a first-class output here
too, same reasoning as ``catalog/connection.py``'s own module docstring
states for ``Connection``.

**Encrypted connections**: ``version_spec`` itself is AES-256-CTR
ciphertext (whole-string, standard base64) whenever the connection has a
vault key, decrypted with the same DEK as chunk-pool/``aHlT`` encryption
before parsing — see ``parse_version_spec``, also used by
``units.saas.object_name_index``. Not documented in ``docs/`` at all
(neither this column's schema nor its encryption).
"""

from __future__ import annotations

import binascii
import dataclasses
import json
from datetime import UTC, datetime

from ..dedup.repository import DedupRepo
from ..errors import KeyMaterialError, NotFoundError
from ..format.crypto import decrypt_version_spec
from ..identifiers import (
    ConnectionConfigId,
    SaasVersionId,
    SnapshotUuid,
    StreamUuid,
    TargetId,
    VersionId,
    VersionUid,
    WorkloadId,
)
from ..storage.base import join_path
from ..storage.sqlite_source import SqliteSource
from ..storage.table import Column, Table, as_int, as_str, sql_placeholders
from .workload import Workload


@dataclasses.dataclass(frozen=True)
class VersionMeta:
    """A version's ``_meta`` sidecar, when present: its directory, the
    filenames it holds, and its own status code."""

    target_meta_path: str
    meta_filenames: tuple[str, ...]
    status: int


@dataclasses.dataclass(frozen=True)
class Version:
    """One ``db/copy_target_version`` row (+ ``_meta`` when present)."""

    version_id: VersionId
    version_uid: VersionUid
    workload_id: WorkloadId
    connection_config_id: ConnectionConfigId
    target_type: str
    target_id: TargetId
    saas_stream_uuid: StreamUuid
    saas_snapshot_uuid: SnapshotUuid
    saas_version_id: SaasVersionId
    deleted: bool
    display_name: str  # version_spec.status.{start,end}_time in local time; "YYYY-MM-DD HH:MM:SS"
    meta: VersionMeta | None


_VERSION_COLUMNS = [
    Column("version_id"),
    Column("version_uid"),
    Column("workload_id"),
    Column("connection_config_id"),
    Column("target_type"),
    Column("target_id"),
    Column("saas_stream_uuid"),
    Column("saas_snapshot_uuid"),
    Column("saas_version_id"),
    Column("deleted"),
    Column("version_spec"),
]


#: ``version_spec.status.status`` values that mean "this version has real,
#: landed data worth listing" — present and uniform across every
#: ``target_type`` (VM/PC/PS/FS/GW/M365 all carry the same field).
#: ``CANCELED`` still belongs here: a canceled backup job can
#: have transferred and landed real data before it was stopped, so it's
#: kept alongside a normally-finished one rather than hidden — opening it
#: degrades per-item like any other version if a given file/object never
#: made it into the landed data. Every other value (``BACKING_UP``/
#: ``FAILED``/``PAUSED``/``DELETING``/``DELETE_FAILED``/``CLONING``/
#: ``CMS_PROCESSING``/``NONE``) means nothing ever landed for this version
#: — not worth listing at all. A version whose status can't even be
#: determined (undecryptable/unparseable ``version_spec``, or no
#: ``status.status`` key present) is filtered out right alongside those,
#: not kept — unlike ``_version_display_name``'s own "degrade, don't
#: hide" default for the same field, an unconfirmed-landed version is
#: excluded from the list entirely rather than shown and left to fail (or
#: silently under-deliver) once opened.
_BROWSABLE_VERSION_STATUSES = frozenset({"COMPLETED", "PARTIAL", "CANCELED"})


def resolve_meta_filename(meta: VersionMeta, name: str, *, ref: str | None = None) -> str:
    """Confirm ``name`` (e.g. ``"target.db"``) is one of this version's own
    ``meta_filenames`` — the ``copy_meta_file/<dir>`` directory's
    authoritative, write-time-recorded file list (FORMAT-SPEC.md:
    version-meta-mapping) — and return it unchanged.

    **Never a directory scan** — unlike the dedup layer's own
    per-generation files (``.buk``, ``db/<name>``, ...; see
    ``storage/seqid.py``'s ``resolve_seq_file``), files under
    ``copy_meta_file`` are written once by the backup agent and never
    rewritten by the dedup layer.

    Raises:
        NotFoundError: ``name`` was never registered for this version.
    """
    if name not in meta.meta_filenames:
        raise NotFoundError(
            f"{name!r} is not registered in this version's meta_filenames",
            ref=ref,
            spec="FORMAT-SPEC.md: version-meta-mapping",
        )
    return name


def resolve_copy_meta_dir(version: Version, repo_root: str) -> str:
    """The ``copy_meta_file/<dir>`` this VM/FS ``version``'s own meta
    artifacts (``target.db``, ``version.db.zst``) live under, derived
    from ``copy_target_version_meta.target_meta_path`` joined onto
    ``repo_root`` (empty when a store is rooted directly at the vault
    dir, non-empty when the repository was discovered from a parent directory
    — ``Session.discover``).

    Raises:
        NotFoundError: ``version`` has no ``copy_target_version_meta`` row at
            all (no meta directory ever landed for it).
    """
    meta = version.meta
    if meta is None or not meta.target_meta_path:
        raise NotFoundError(
            f"version {version.version_uid} has no copy_target_version_meta row (no meta directory ever landed for it)",
            ref=version.version_uid,
        )
    dirname = meta.target_meta_path.rstrip("/").rsplit("/", 1)[-1]
    return join_path(repo_root, "copy_meta_file", dirname)


async def open_target_db(repo: DedupRepo, version: Version, meta_dir: str) -> SqliteSource:
    """Resolve and open ``<meta_dir>/target.db`` into an opened
    ``SqliteSource`` — the identical sequence ``units.device`` and
    ``units.fs`` each need before going on to interpret ``target.db``'s
    own tables their own way. May be ``aHlT``-enveloped and have a real
    ``-wal``/``-shm`` sidecar (FORMAT-SPEC.md §6.1); ``SqliteSource.
    from_enveloped_store`` handles both. A different addressing scheme
    than ``DedupRepo.db``'s own envelope auto-detection (``db/<name>``;
    see its own docstring), so this resolves the physical filename itself.

    ``rebuild_target.db`` (§6.4) is never read through this function.
    """
    meta = version.meta
    assert meta is not None  # the caller's own meta_dir resolution already required this
    physical_name = resolve_meta_filename(meta, "target.db", ref=f"{meta_dir}/target.db")
    return await SqliteSource.from_enveloped_store(repo.store, f"{meta_dir}/{physical_name}", vault_key=repo.vault_key)


async def versions(repo: DedupRepo, workload: Workload, *, include_deleted: bool = False) -> list[Version]:
    """Newest-first by real backup time (``_version_epoch``, the same
    field/fallback ``_version_display_name`` uses, so sort order and
    displayed timestamp always agree). ``table.select()`` carries no
    ``ORDER BY`` of its own — row order is an implementation detail, not a
    chronological guarantee — so sorting happens here. A version whose
    timestamp can't be resolved sorts last, ties broken by ``version_id``
    descending (the best available recency proxy once the real timestamp
    is unusable)."""
    # Hinted on what the WHERE below actually filters by: the schema's own
    # index is on version_uid, which this query never mentions, so without an
    # index here every call is a full scan of a table carrying version_spec
    # blobs, paid once per workload.
    #
    # Only takes effect where the connection is onto a materialized copy (a
    # remote store, or a local one with a live -wal). A plain local repository
    # takes open_sqlite's fast path onto the real file, immutable and
    # read-only, where apply_index_hint is documented to do nothing -- that
    # case still scans.
    table = await Table.create(
        await repo.db("copy_target_version"),
        "copy_target_version",
        _VERSION_COLUMNS,
        index_hints=[["workload_id", "deleted"]],
    )
    where = "workload_id = ?" if include_deleted else "workload_id = ? AND deleted = 0"
    # Two passes: the first collects every row that survives the
    # browsable-status filter, the second batch-resolves their
    # copy_target_version_meta rows in one query — mirrors
    # catalog/connection.py's own _namespace_by_workload()'s
    # WHERE ... IN (...) batching.
    rows: list[tuple[dict[str, object], str, ParsedVersionStatus | None]] = []
    async for row in table.select(where, (workload.workload_id,)):
        version_uid = as_str(row["version_uid"])
        version_spec_raw = as_str(row["version_spec"])
        status = _parse_version_status(version_spec_raw, version_uid, repo.vault_key)
        status_value = status.status if status is not None else None
        if not isinstance(status_value, str) or status_value not in _BROWSABLE_VERSION_STATUSES:
            continue
        rows.append((row, version_uid, status))
    metas = await _version_metas_for(repo, [version_uid for _row, version_uid, _status in rows])
    result: list[tuple[int | None, int, Version]] = []
    for row, version_uid, status in rows:
        target_type = as_str(row["target_type"])
        version_id = as_int(row["version_id"])
        epoch = _version_epoch(status)
        result.append(
            (
                epoch,
                version_id,
                Version(
                    version_id=VersionId(version_id),
                    version_uid=VersionUid(version_uid),
                    workload_id=WorkloadId(as_int(row["workload_id"])),
                    connection_config_id=ConnectionConfigId(as_int(row["connection_config_id"])),
                    target_type=target_type,
                    target_id=TargetId(as_str(row["target_id"])),
                    saas_stream_uuid=StreamUuid(as_str(row["saas_stream_uuid"])),
                    saas_snapshot_uuid=SnapshotUuid(as_str(row["saas_snapshot_uuid"])),
                    saas_version_id=SaasVersionId(as_int(row["saas_version_id"])),
                    deleted=bool(row["deleted"]),
                    display_name=_version_display_name(status, version_uid),
                    meta=metas.get(version_uid),
                ),
            )
        )
    result.sort(key=lambda item: (item[0] is not None, item[0] or 0, item[1]), reverse=True)
    return [version for _epoch, _version_id, version in result]


def _as_epoch_seconds(value: object) -> int | None:
    """Narrow one of ``version_spec.status``'s ``start_time``/``end_time``
    values to a usable epoch — real data has these as **strings**
    (protobuf-JSON's own int64-as-string convention,
    e.g. ``"1786024626"``), and ``"0"``/``0`` is the proto's own "not
    set" sentinel (``status.copy_time``'s own field comment says so
    explicitly; the same convention applies to ``start_time``/
    ``end_time``), so both parse failure and the zero sentinel return
    ``None`` — never ``0`` itself, which would format as 1970."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value or None
    if isinstance(value, str):
        try:
            parsed = int(value)
        except ValueError:
            return None
        return parsed or None
    return None


@dataclasses.dataclass(frozen=True)
class ParsedVersionStatus:
    """``version_spec.status``, narrowed to exactly the fields
    ``_version_epoch``/``_version_display_name``/the browsable-status filter
    (``_BROWSABLE_VERSION_STATUSES``) read — a type-checked replacement for
    passing a raw ``dict[str, Any]`` around, so mypy enforces "call
    ``_parse_version_status`` first" as a real requirement instead of a
    documented-only convention. Field names match
    ``version_spec.status``'s own JSON keys."""

    status: object = None
    start_time: object = None
    end_time: object = None


def parse_version_spec(version_spec_raw: str, version_uid: str, vault_key: bytes | None) -> object | None:
    """Decrypt (whenever ``vault_key`` is given) and JSON-parse one
    ``copy_target_version.version_spec`` column value — the shared
    decrypt-then-parse step this module's own status filter and
    ``units.saas.object_name_index``'s connector-recorded index both need.

    Decrypts unconditionally whenever ``vault_key`` is given, never by
    probing the raw column first (§5.1: encryption is a per-connection
    state, not per-row).

    Returns ``None`` on any failure — undecryptable or unparseable —
    never raises; each caller applies its own conservative default."""
    try:
        raw = (
            decrypt_version_spec(version_spec_raw, version_uid, vault_key)
            if vault_key is not None
            else version_spec_raw
        )
        parsed: object = json.loads(raw)
    except (ValueError, UnicodeDecodeError, binascii.Error, KeyMaterialError):
        return None
    return parsed


def _parse_version_status(
    version_spec_raw: str, version_uid: str, vault_key: bytes | None
) -> ParsedVersionStatus | None:
    """``parse_version_spec``'s result, narrowed to its ``status`` object —
    the one decrypt+parse this module needs per row, shared by the status
    filter (``_BROWSABLE_VERSION_STATUSES``) and ``_version_display_name``.

    ``None`` on any failure (undecryptable, unparseable, or no ``status``
    present) — each caller picks its own conservative default for that
    case."""
    spec = parse_version_spec(version_spec_raw, version_uid, vault_key)
    raw_status = spec.get("status") if isinstance(spec, dict) else None
    if not isinstance(raw_status, dict):
        return None
    return ParsedVersionStatus(
        status=raw_status.get("status"),
        start_time=raw_status.get("start_time"),
        end_time=raw_status.get("end_time"),
    )


def _version_epoch(status: ParsedVersionStatus | None) -> int | None:
    """Real backup time: ``status.start_time``, falling back to
    ``end_time`` only when ``start_time`` itself is absent/zero — both are
    unix-epoch seconds from the *source* backup job. ``None`` when
    ``status`` is ``None`` or neither field yields a usable epoch (see
    ``_as_epoch_seconds``). Shared by ``_version_display_name`` and
    ``versions`` so both use the same rule rather than risking two copies
    drifting apart."""
    if status is None:
        return None
    epoch = _as_epoch_seconds(status.start_time)
    if epoch is None:
        epoch = _as_epoch_seconds(status.end_time)
    return epoch


def _version_display_name(status: ParsedVersionStatus | None, version_uid: str) -> str:
    """Formats ``_version_epoch``'s real backup time for display.

    Degrades to the raw ``version_uid`` on any failure (undecryptable,
    unparseable, or no usable timestamp) — deliberately *not* a crtime
    fallback: ``version_spec`` is expected to always be present and
    parseable, so this path is a rare degrade for corrupt data, not a
    normal branch to design a second timestamp source around.
    """
    epoch = _version_epoch(status)
    if epoch is None:
        return version_uid
    return datetime.fromtimestamp(epoch, UTC).astimezone().strftime("%Y-%m-%d %H:%M:%S")


async def _version_metas_for(repo: DedupRepo, version_uids: list[str]) -> dict[str, VersionMeta]:
    """Batched ``copy_target_version_meta`` lookup — one ``WHERE
    version_uid IN (...)`` query for every uid in ``version_uids``,
    mirroring ``catalog/connection.py``'s own ``_namespace_by_workload``'s
    batching shape. A ``version_uid`` with no matching row (or no
    ``copy_target_version_meta`` table at all) is simply absent from
    the returned dict."""
    if not version_uids:
        return {}
    try:
        conn = await repo.db("copy_target_version_meta")
    except NotFoundError:
        return {}
    placeholders = sql_placeholders(len(version_uids))
    cursor = await conn.execute(
        "SELECT version_uid, target_meta_path, meta_filenames, status FROM copy_target_version_meta "
        f"WHERE version_uid IN ({placeholders})",
        version_uids,
    )
    metas: dict[str, VersionMeta] = {}
    for version_uid, target_meta_path, meta_filenames_json, status in await cursor.fetchall():
        # meta_filenames_json can be NULL for a row still mid-upload
        # (copy_target_version_meta.status == 0, "Writing" -- 1 is
        # "Complete", FORMAT-SPEC.md: version-meta-mapping): treat that
        # the same as "no files landed yet" rather than letting
        # json.loads(None) crash every version's lookup in the same
        # batched query.
        metas[version_uid] = VersionMeta(
            target_meta_path=target_meta_path,
            meta_filenames=tuple(json.loads(meta_filenames_json or "[]")),
            status=status,
        )
    return metas
