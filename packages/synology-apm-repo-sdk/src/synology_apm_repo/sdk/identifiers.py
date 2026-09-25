"""Distinct identifier spaces that look identical on the wire (all ``int`` or
all ``str``) but must never be mixed.

Declaring each as a ``typing.NewType`` costs nothing at runtime — these are
plain ``int``/``str`` at the bytes level — and turns "silently read the wrong
repository file because two same-shaped identifiers got swapped" from a
support incident into a mypy error, since ``mypy --strict`` is already
enabled for this project.
"""

from __future__ import annotations

from typing import NewType

# --- dedup addressing (FORMAT-SPEC.md: ChunkAddress, composition-splitting, file_map-relationship) --------------------

StreamId = NewType("StreamId", int)
"""``ChunkAddress`` / composition-path stream id (0-255, ``uint8``)."""

SessionId = NewType("SessionId", int)
"""Composition-path session id — one per backup version within a stream."""

CompOffset = NewType("CompOffset", int)
"""``db/file_map.comp_offset`` — the global offset of a ``RecordHead``
within a ``(StreamId, SessionId)`` composition."""

BucketId = NewType("BucketId", int)
"""Pool bucket id (0..2**40-1) — part of ``ChunkAddress``."""

ChunkIdx = NewType("ChunkIdx", int)
"""Index of a 4 KiB chunk within a bucket (0..8191) — part of
``ChunkAddress``."""

# --- catalog (db/connection_config, db/workload_config, ...) ---------------

ConnectionId = NewType("ConnectionId", str)
"""``db/connection_config.connection_id`` — 12-char id identifying a Copy /
Tiering connection. NOT the same space as ``ConnectionConfigId``."""

ConnectionConfigId = NewType("ConnectionConfigId", int)
"""``db/connection_config.connection_config_id`` — the local autoincrement
primary key for a connection *and version_type* pair."""

CatalogId = NewType("CatalogId", str)
"""Repository Layer identifier for one ``api.Catalog`` — part of the
``Session``/``Repository``/``Catalog`` split that CLI/TUI code imports
directly, with everything below that split treated as an implementation
detail — distinct from every id above in that it's not a raw
on-disk column: a vault's own catalogs share one ``connection_config`` table
(unique *within* that vault, so ``str(connection_config_id)`` alone
identifies one), but each object-storage sibling's own ``connection_config``
table independently starts back at 1 — a bare ``connection_config_id`` can't
tell two siblings apart. For a vault, this is ``str(connection.connection_config_id)``;
for object storage, the repo-id string itself (already unique per bucket by
construction, being a directory name)."""


def resolve_catalog_id(repo_id: str | None, connection_config_id: ConnectionConfigId) -> CatalogId:
    """The one shared place holding the ``CatalogId`` fallback formula —
    ``repo_id`` when set (object storage), else ``str(connection_config_id)``
    (a vault) — used both by ``api.catalog.Catalog.catalog_id``
    and ``units.node_ref.canonical_ref_for``, which would otherwise each
    carry an identical, independently-drifting copy of this fallback."""
    return CatalogId(repo_id or str(connection_config_id))


WorkloadId = NewType("WorkloadId", int)
"""``db/workload_config.workload_id`` — local autoincrement primary key."""

WorkloadUid = NewType("WorkloadUid", str)
"""``db/workload_config.workload_uid`` — ``"<uuid>;<serial>"`` composite."""

VersionId = NewType("VersionId", int)
"""``db/copy_target_version.version_id`` — local autoincrement primary key."""

SaasVersionId = NewType("SaasVersionId", int)
"""``copy_target_version.saas_version_id`` / ``saas_version.version_info.version_id``
— a SaaS stream's *own*, per-stream version numbering (resolved via
``(snapshot_id, SaasVersionId) -> stream_version``, ``units/saas/stream.py``).
NOT the same space as ``VersionId`` above — both are ``int``, and
``catalog.Version`` carries one of each."""

VersionUid = NewType("VersionUid", str)
"""``db/copy_target_version.version_uid`` — the Copy-version UUID minted
server-side; forms ``copy_meta_file/<Type>_<VersionUid>``. NOT the same space as
``target.db``'s on-prem-minted ``version_table.version_uuid``."""

SnapshotUuid = NewType("SnapshotUuid", str)
"""``saas_snapshot.snapshot_info.snapshot_uuid`` — server-minted snapshot id."""

TargetId = NewType("TargetId", str)
"""``db/copy_target_version.target_id`` — the on-prem workload UUID (device
workloads) or stream uuid (SaaS)."""

StreamUuid = NewType("StreamUuid", str)
"""SaaS dedup-stream uuid (16 chars) — ``saas/<ccid>/<StreamUuid>/``."""
