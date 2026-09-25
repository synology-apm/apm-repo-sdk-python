"""``DeviceProvider``: VM/PC/PS workloads via ``copy_meta_file``.

The dedup engine's own smallest recognized unit for these workload types
is still a whole disk/volume image (FORMAT-SPEC.md: copy_meta_file-layout) — this
provider's disk-image leaves are exactly that, unchanged, and every
existing ref pointing at one keeps working identically. Every dedup
disk-image object also gets one additional, purely-additive sibling node —
"``<name>`` (filesystem)" — built by ``units.content.disk_fs``
(Dissect-framework-based) via ``units.device_disk_fs``, that parses the
guest OS's own filesystem
(NTFS/FAT/exFAT/ext2-4/XFS/Btrfs/APFS) inside that same disk image and
offers individual guest files as real, browsable/exportable
``RestorableUnit``\\ s. When Dissect can't recognize any filesystem on a given
disk (encrypted, unsupported, or genuinely not a filesystem-bearing
image), that sibling node is simply absent or shows one diagnostic leaf
explaining why — the whole-image node is completely unaffected either
way.

Two distinct paths, dispatched **directly on ``Version.target_type``**
(``"VM"`` vs ``{"PC", "PS"}``), never by probing which files exist — a
version's ``target_type`` is already known the moment a ``Version`` is
constructed (``db/copy_target_version.target_type``, see ``catalog/version.py``):

- **VM/FS**: ``target.db`` → its ``version_table`` row → ``device_table``
  (one row per disk device) → ``object_table`` (one row per disk image,
  joined on ``config_device_id``). Handled directly in this module.
- **PC/PS**: the landing directory has no ``target.db``, only
  ``snapshot_info.json`` (FORMAT-SPEC.md: copy_meta_file-layout). The disk list
  instead comes from the destination repository's own ``db/copy_target_file``
  (``version_id`` → ``fid``) joined with ``db/file_meta`` (``fid`` → ``path``),
  looked up in ``db/file_map`` through the same bridge the VM path uses —
  the same production code path populates both tables for VM and PC/PS
  alike. Handled by ``device_pcps.PcpsDiskTree``, a collaborator
  ``DeviceProvider`` delegates to because one physical PC/PS disk can land
  as several independently-registered fragment objects, not one, and
  ``PcpsDiskTree`` owns both the grouping rule and reassembling a group's
  fragments into one disk.

**Trap:** a PC/PS landing directory exists for every normally-landed
version too, just without ``target.db`` — so "does ``copy_meta_file/<dir>``
exist" can't distinguish VM from PC/PS. ``target_type`` is the only
reliable dispatch signal.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from types import TracebackType
from typing import Self

from ..catalog.version import Version, open_target_db, resolve_copy_meta_dir
from ..catalog.workload import TargetType
from ..dedup.dedup_file import DEFAULT_STREAM_BLOCK, ExportResult, stream_via_read
from ..dedup.repository import DedupRepo
from ..errors import NotFoundError, UnsupportedDataFormatError
from ..storage.sqlite import apply_index_hint
from ..storage.sqlite_source import SqliteSource
from .base import (
    ContentSource,
    Node,
    RestorableUnit,
    UnitKind,
    disk_fs_containers_before_leaves,
    not_restorable,
    paginate,
)
from .content.disk_fs import disk_fs_available
from .device_disk_fs import DiskFsSibling
from .device_kind import _NodeKind
from .device_pcps import PcpsDiskTree
from .node_ref import NodeRef, canonical_ref_for

_DATA_FORMAT_DEDUP = 1


class DeviceProvider:
    """``UnitProvider`` for one VM/PC/PS workload version.

    **Build one with** ``create``, never ``DeviceProvider(...)`` directly —
    kept as the documented construction path even though it does no I/O
    (``_is_pcps`` is a pure function of ``version.target_type``, decided once
    and cached rather than re-derived on every access), so ``root`` stays
    synchronous.
    """

    def __init__(self, repo: DedupRepo, version: Version) -> None:
        """Pure field initialization. ``_is_pcps`` is derived from
        ``version.target_type`` — the only reliable VM/PC-PS signal, since
        file presence alone can't distinguish the two."""
        self._repo = repo
        self._version = version
        self._meta_dir: str | None = None
        self._target_db: SqliteSource | None = None
        self._is_pcps = version.target_type != TargetType.VM
        # The two collaborators this class delegates PC/PS listing and
        # the disk-fs sibling axis to. Each is scoped to this one
        # provider instance and owns its own per-disk caches.
        self._pcps = PcpsDiskTree(self)
        self._disk_fs = DiskFsSibling(self)

    @classmethod
    async def create(cls, repo: DedupRepo, version: Version) -> Self:
        return cls(repo, version)

    # -- shared accessors for PcpsDiskTree/DiskFsSibling ---------------

    @property
    def repo(self) -> DedupRepo:
        """Exposed for ``PcpsDiskTree``/``DiskFsSibling``, the two
        collaborators this class delegates to."""
        return self._repo

    @property
    def version(self) -> Version:
        """Same reason as ``repo``."""
        return self._version

    @property
    def disk_fs(self) -> DiskFsSibling:
        """The disk-fs sibling collaborator — exposed so ``PcpsDiskTree``
        can build a PC/PS disk's own "(filesystem)" sibling node the
        same way this class's own ``_object_nodes`` does for a VM disk."""
        return self._disk_fs

    def extra_ref(self, *extra: str) -> NodeRef:
        """Append to this provider's own version ref rather than nesting
        ``str(self._version_ref())`` as a new ``repo_path`` — the latter
        bakes a literal ``#`` into the middle of the ref string, which
        ``NodeRef.parse()`` (splitting on the *first* ``#`` only) then
        silently swallows into the ``version_uid`` segment on round-trip.
        Every other provider uses this same flat form via
        ``NodeRef.canonical(..., extra=...)``. Public — also used by
        ``PcpsDiskTree``."""
        return self._version_ref().child(*extra)

    # -- copy_meta_file location (VM/FS only) --------------------------

    def _resolve_meta_dir(self) -> str:
        """The ``copy_meta_file/<dir>`` this VM/FS version's ``target.db``
        lives under. VM-only — PC/PS has no ``target.db`` (module
        docstring) and never calls this.

        Synchronous: ``resolve_copy_meta_dir()`` is pure, no-I/O lookup
        work, the same reason ``units/fs.py``'s identically-shaped
        ``_resolve_meta_dir`` stays sync.

        Raises:
            NotFoundError: This version has no ``copy_target_version_meta``
                row at all (no meta directory ever landed).
        """
        if self._meta_dir is not None:
            return self._meta_dir
        path = resolve_copy_meta_dir(self._version, self._repo.layout.repo_root)
        self._meta_dir = path
        return path

    async def _target_db_source(self) -> SqliteSource:
        if self._target_db is not None:
            return self._target_db
        meta_dir = self._resolve_meta_dir()
        self._target_db = await open_target_db(self._repo, self._version, meta_dir)
        return self._target_db

    async def close(self) -> None:
        """Release the sqlite connection(s) this provider opened.

        Not merely for tidiness: an ``aiosqlite`` connection owns a
        background worker thread created **without** ``daemon=True``, so a
        provider abandoned without closing keeps the interpreter alive
        forever in ``threading._shutdown``. ``Repository.close()`` calls
        this for every provider it handed out, so ordinary callers never
        have to.
        """
        if self._target_db is not None:
            await self._target_db.close()
            self._target_db = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    # -- UnitProvider -------------------------------------------------

    def root(self) -> Node:
        """Pure construction, no I/O — ``_is_pcps`` was already resolved
        in ``__init__``. Carries no ``degraded``/caveat attrs: whether this
        version's objects actually resolve is only knowable by querying
        ``copy_target_file``/``file_meta``/``file_map``, which
        ``PcpsDiskTree.object_nodes`` does lazily, per call, not here."""
        if self._is_pcps:
            return Node(ref=self._version_ref(), name="Disks", is_leaf=False, attrs={"_kind": _NodeKind.PCPS_ROOT})
        return Node(ref=self._version_ref(), name="Devices", is_leaf=False, attrs={"_kind": _NodeKind.ROOT})

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        kind = node.attrs.get("_kind")
        match kind:
            case _NodeKind.ROOT:
                return await self._device_nodes(offset=offset, limit=limit)
            case _NodeKind.DEVICE:
                return await self._object_nodes(node.attrs["config_device_id"], offset=offset, limit=limit)
            case _NodeKind.PCPS_ROOT:
                return await self._pcps.object_nodes(offset=offset, limit=limit)
            case _NodeKind.DISK_FS_ROOT | _NodeKind.DISK_FS_ENTRY:
                return await self._disk_fs.children(node, offset=offset, limit=limit)
            case _:
                return []

    async def unit(self, node: Node) -> RestorableUnit:
        kind = node.attrs.get("_kind")
        match kind:
            case _NodeKind.OBJECT:
                return await self._open_object(node)
            case _NodeKind.PCPS_DISK:
                return await self._pcps.open_disk(node)
            case _NodeKind.DISK_FS_ENTRY:
                return await self._disk_fs.open_entry(node)
            case _NodeKind.PCPS_DIAGNOSTIC:
                # Not a real restorable object — attrs["diagnostic"] already
                # holds the human-readable summary of which registered fids
                # never resolved.
                raise NotFoundError(node.attrs["diagnostic"], ref=str(node.attrs["missing_fids"]))
            case _NodeKind.DISK_FS_DIAGNOSTIC:
                # Not a real restorable object either — attrs["diagnostic"]
                # already holds the human-readable explanation of why no
                # filesystem was recognized.
                raise NotFoundError(node.attrs["diagnostic"], ref=self._version.version_uid)
            case _:
                not_restorable("node", node.name)

    # -- VM path ----------------------------------------------------------

    def _version_ref(self) -> NodeRef:
        return canonical_ref_for(self._repo, self._version)

    async def _device_nodes(self, *, offset: int = 0, limit: int | None = None) -> list[Node]:
        # Catalog.versions() lists every VM version regardless of meta
        # availability (api/catalog.py's Catalog.versions() docstring), so a
        # version that never registered target.db at all, or whose
        # copy_meta_file/<dir> has since been removed from the store,
        # routinely raises NotFoundError here (via _target_db_source() ->
        # catalog/version.py's resolve_copy_meta_dir/resolve_meta_filename)
        # — a normal degrade the caller (CLI/TUI) already handles, not a bug
        # to chase. Left to propagate as a plain error, same as any other
        # unresolvable-content case in this codebase.
        conn = (await self._target_db_source()).connection
        # ORDER BY host_name (the only column here with real display
        # meaning; there's no date-like column to prefer instead) with
        # device_id (the table's own real rowid-alias PK, not the
        # business-facing config_device_id) as the deterministic
        # pagination tiebreaker.
        cursor = await conn.execute(
            "SELECT config_device_id, device_uuid, host_name, os_name FROM device_table "
            "ORDER BY host_name, device_id LIMIT ? OFFSET ?",
            (limit if limit is not None else -1, offset),
        )
        rows = await cursor.fetchall()
        nodes = []
        for config_device_id, device_uuid, host_name, os_name in rows:
            ref = self.extra_ref(f"device:{config_device_id}")
            nodes.append(
                Node(
                    ref=ref,
                    name=host_name,
                    is_leaf=False,
                    attrs={
                        "_kind": _NodeKind.DEVICE,
                        "config_device_id": config_device_id,
                        "device_uuid": device_uuid,
                        "os_name": os_name,
                    },
                )
            )
        return nodes

    async def _object_nodes(self, config_device_id: int, *, offset: int = 0, limit: int | None = None) -> list[Node]:
        conn = (await self._target_db_source()).connection
        version_cursor = await conn.execute("SELECT version_id FROM version_table")
        version_row = await version_cursor.fetchone()
        if version_row is None:
            raise NotFoundError("target.db has no version_table row", ref=self._meta_dir)
        version_id = version_row[0]
        # object_table only has IDX_PARENT_OBJ_ID (on parent_object_id) in
        # the real schema — nothing covers (version_id, config_device_id)
        # itself. A no-op in practice (this table is already scoped to
        # one version's own devices, typically small) but applied
        # unconditionally for consistency — safe even when the columns
        # are already covered by an existing index (a cheap leading-
        # prefix check skips it) or the connection is read-only (CREATE
        # INDEX then just raises, caught and treated as a no-op).
        await apply_index_hint(conn, "object_table", ["version_id", "config_device_id"])
        # Fetched unpaginated and sliced in Python below — same reasoning
        # PcpsDiskTree._build_nodes() already has for its own per-key node
        # count not matching its own row count 1:1: a dedup,
        # non-unsupported object below contributes *two* nodes (the disk
        # image plus its "(filesystem)" sibling), so a SQL-level
        # LIMIT/OFFSET against object_table's own row count wouldn't line
        # up with this method's actual returned node count. Real devices
        # register a handful of disk objects, never thousands, so this
        # stays cheap.
        # coverage.py attributes this multi-line call's hit to its closing
        # line, not this opening one, so this line is flagged uncovered
        # even though it always executes.
        cursor = await conn.execute(  # pragma: no cover
            "SELECT object_id, data_format, file_path, src_file_path, temp_postfix, dedup_object, file_size "
            "FROM object_table WHERE version_id = ? AND config_device_id = ? "
            "AND (temp_postfix IS NULL OR temp_postfix = '') "
            "ORDER BY file_path, object_id",
            (version_id, config_device_id),
        )
        rows = await cursor.fetchall()
        nodes = []
        for object_id, data_format, file_path, src_file_path, _temp_postfix, dedup_object, file_size in rows:
            leaf_name = file_path.rsplit("/", 1)[-1] if file_path else f"object-{object_id}"
            ref = self.extra_ref(f"device:{config_device_id}", f"object:{object_id}")
            unsupported = bool(dedup_object) and data_format != _DATA_FORMAT_DEDUP
            object_node = Node(
                ref=ref,
                name=leaf_name,
                is_leaf=True,
                kind=UnitKind.DISK_IMAGE if dedup_object else UnitKind.FILE,
                size=file_size,
                attrs={
                    "_kind": _NodeKind.OBJECT,
                    "object_id": object_id,
                    "data_format": data_format,
                    "file_path": file_path,
                    "src_file_path": src_file_path,
                    "dedup_object": bool(dedup_object),
                    "unsupported": unsupported,
                },
            )
            nodes.append(object_node)
            if dedup_object and not unsupported and disk_fs_available():
                nodes.append(
                    self._disk_fs.root_node(disk_key=("object", object_id), source_node=object_node, name=leaf_name)
                )
        # disk_fs_containers_before_leaves(): each "(filesystem)" sibling
        # keeps its own file_path/object_id-ordered position relative to
        # its peers.
        return paginate(disk_fs_containers_before_leaves(nodes), offset, limit)

    async def _open_object(self, node: Node) -> RestorableUnit:
        attrs = node.attrs
        content: ContentSource
        if attrs["dedup_object"]:
            if attrs["data_format"] != _DATA_FORMAT_DEDUP:
                raise UnsupportedDataFormatError(
                    f"object {attrs['object_id']} has unsupported data_format {attrs['data_format']} "
                    "(CBT chains are not yet supported — v1 refuses rather than return wrong content)",
                    ref=attrs["src_file_path"],
                )
            location = await self._repo.locate_file(attrs["src_file_path"])
            content = self._repo.open_composition(
                location.stream_id, location.session_id, location.comp_offset, size=node.size
            )
        else:
            meta_dir = self._resolve_meta_dir()
            content = await _LocalFileContentSource.create(self._repo, f"{meta_dir}/{attrs['file_path']}", node.size)
        return RestorableUnit(
            ref=node.ref, name=node.name, is_leaf=True, kind=node.kind, size=node.size, attrs=attrs, content=content
        )


class _LocalFileContentSource:
    """A ``ContentSource`` reading a file directly from the store — not
    through the dedup layer at all — for the non-dedup descriptor files
    (``.vmx``/``.vmdk`` headers/``.delta``) that live plainly under
    ``copy_meta_file/<dir>/``.

    Build one with ``create``: ``size`` keeps ``ContentSource``'s sync ``size``
    property, so a caller-unknown size has to be resolved with a real
    ``store.size()`` call *before* the object exists, not lazily inside
    the property."""

    def __init__(self, repo: DedupRepo, path: str, size: int | None) -> None:
        self._repo = repo
        self._path = path
        self._size = size

    @classmethod
    async def create(cls, repo: DedupRepo, path: str, size: int | None) -> Self:
        return cls(repo, path, size if size is not None else await repo.store.size(path))

    @property
    def size(self) -> int | None:
        return self._size

    @property
    def supports_concurrent_export(self) -> bool:
        """Always ``False`` — a plain store-level file read has no bucket
        concept to spread reads across."""
        return False

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        return await self._repo.store.read(self._path, offset, length)

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]:
        return stream_via_read(self, block)

    async def export_to(
        self,
        dst: Path,
        *,
        sparse: bool = True,
        progress: Callable[[int, int], Awaitable[None]] | None = None,
    ) -> ExportResult:
        # The non-dedup sidecar files (.vmx/.vmdk headers, .delta) are
        # plain files, not dedup content — no sparse/hole concept
        # applies, so this is always a full, non-sparse copy regardless
        # of the ``sparse`` flag. Returning ExportResult (not None) matters
        # beyond type-signature tidiness: a caller treating every
        # ContentSource uniformly (e.g. the CLI's ``export`` command) needs
        # a real result to report, not a silent None.
        assert self.size is not None
        data = await self.read(0, self.size)
        # Local file writes have no native async form in CPython (no
        # asyncio equivalent of os.open/os.write) — the blocking call
        # goes through to_thread, same as dedup/dedup_file.py's own
        # export path.
        await asyncio.to_thread(Path(dst).write_bytes, data)
        return ExportResult(bytes_written=self.size, logical_size=self.size, holes=0, zeros=0)
