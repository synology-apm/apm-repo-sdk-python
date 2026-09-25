"""Shared restorable-unit vocabulary. Every workload provider (Device, FS,
SaaS, ...) implements ``UnitProvider`` against these same three types, so
the CLI/TUI never need to know which workload they're looking at.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, NoReturn, Protocol, Self, TypeVar, runtime_checkable

from ..dedup.dedup_file import DEFAULT_STREAM_BLOCK
from ..storage.table import as_int
from .node_ref import NodeRef

_T = TypeVar("_T")


class UnitKind(enum.Enum):
    """Which restorable-unit kind a ``Node``/``RestorableUnit`` is —
    drives the CLI/TUI's default icon/preview routing."""

    DISK_IMAGE = "disk_image"
    # Cosmetic only (TUI icon/preview routing) — a disk-image node with a
    # "(filesystem)" sibling, or a folder inside that sibling's own
    # tree, still browses/exports identically without this.
    DISK_FILESYSTEM = "disk_filesystem"
    DISK_FILE = "disk_file"
    FILE = "file"
    MAIL = "mail"
    CONTACT = "contact"
    CALENDAR_EVENT = "calendar_event"
    DRIVE_ITEM = "drive_item"
    SITE_ITEM = "site_item"
    RAW_OBJECT = "raw_object"
    #: A Teams channel or Chat conversation's own rendered transcript page
    #: — distinct from ``RAW_OBJECT`` (``RawObjectProvider``'s unrelated
    #: raw diagnostic listing) and from ``CATEGORY_GROUP`` (never a
    #: leaf's own kind, only ever a container's ``leaf_kind``): this
    #: value is both a leaf's own kind and every ``TeamsChatProvider``
    #: container's ``leaf_kind`` (root, each channel category) alike.
    TEAMS_CHAT_MESSAGE = "teams_chat_message"
    #: A container whose own children are always further containers, never
    #: a leaf — a SharePoint site's "List" category (each child a List's
    #: own group node) and a Calendar's "My"/"Other Calendars" category
    #: (each child an individual calendar's own group node). Shared across
    #: providers because the shape it describes is identical in both:
    #: Name+Created columns, and never ``ColumnSpec.leaves_only`` (its
    #: children being containers is exactly what a folder listing should
    #: show, not filter out).
    CATEGORY_GROUP = "category_group"


class FileState(enum.Enum):
    """A disk-fs file's cloud-sync/encryption state, carried as
    ``Node.attrs["file_state"]`` — currently produced only by
    ``units/content/disk_fs/``'s NTFS/APFS backends (``_ntfs.py``/
    ``_apfs.py``), not a concept
    every provider kind has. In priority order when more than one
    applies: ``CLOUD_ONLY`` always wins over ``ENCRYPTED`` — an evicted
    cloud placeholder has no local bytes at all, so its export fails
    regardless of whether it's also EFS-encrypted."""

    NORMAL = "normal"
    ENCRYPTED = "encrypted"
    CLOUD_ONLY = "cloud_only"


@runtime_checkable
class ContentSource(Protocol):
    """The one content-reading contract every restorable unit's ``open()``
    returns — a ``DedupFile``/``ByteRangeView`` satisfies this shape already;
    an ``ArtifactBuilder`` (assembled content — ``.eml``, ``.ics``, ...) can
    implement it too without CLI/TUI ever needing to tell them apart."""

    @property
    def size(self) -> int | None:
        """Deliberately a ``@property``, not a plain attribute. Protocol
        attributes are checked invariantly, so a plain ``int | None`` field
        would reject a narrower ``int`` attribute (e.g. ``ByteRangeView``'s
        ``size: int``); a property satisfies this structurally regardless.

        Synchronous, even in this async SDK — every ``DedupFile``-backed
        implementer already knows its size without I/O. ``LazyArtifact`` is
        the one implementer that can't; it reports ``None`` until its
        artifact is assembled, which ``int | None`` exists for."""
        ...

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        """Read ``length`` bytes starting at ``offset`` (default: from
        ``offset`` to the end). A request extending past the content's own
        end — including ``offset`` starting at or past it — returns only
        the bytes that exist, never an error; a genuinely empty result is
        a normal, valid read, not a failure.

        Raises:
            ValueError: ``offset`` or ``length`` is negative.
        """
        ...

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes]]: ...

    async def export_to(
        self,
        dst: Path,
        *,
        sparse: bool = True,
        progress: Callable[[int, int], Awaitable[None]] | None = None,
    ) -> object: ...

    @property
    def supports_concurrent_export(self) -> bool:
        """Whether ``export_to()`` accepts ``max_concurrent_reads``/
        ``max_concurrent_opens`` meaningfully — true only for a source with
        a bucket concept to spread reads across (``DedupFile``/
        ``ByteRangeView``, ``VirtualDiskContentSource``). Declared explicitly
        here, not inferred by ``isinstance`` against a concrete class, so
        every implementation states its own capability directly."""
        ...


@dataclasses.dataclass(frozen=True)
class Node:
    """One entry in a provider's browsable tree; leaves additionally
    carry ``kind``/``size``.

    Attributes:
        ref: This node's canonical reference.
        name: Display name.
        is_leaf: Whether this node is a restorable unit rather than a
            container.
        kind: The kind of restorable unit, when known.
        size: Byte size, when known.
        attrs: Display metadata (sender, mtime, path, ...) — CLI/TUI show
            *only* ``name``/``attrs`` in the default, non-diagnostic mode;
            internal identifiers a provider needs for its own bookkeeping
            should go in a provider-private ``attrs`` key, not become part
            of this public shape.
    """

    ref: NodeRef
    name: str
    is_leaf: bool
    kind: UnitKind | None = None
    size: int | None = None
    attrs: dict[str, Any] = dataclasses.field(default_factory=dict)


def node_file_state(node: Node) -> FileState:
    """``node.attrs["file_state"]`` narrowed to a real ``FileState``,
    defaulting to ``NORMAL`` for a node with no such concept (most
    provider kinds) or a malformed value — ``Node.attrs`` is an untyped
    ``dict[str, Any]``, so this is the one narrowing every CLI/TUI
    render site needs, instead of repeating the same check at each of
    them."""
    state = node.attrs.get("file_state")
    return state if isinstance(state, FileState) else FileState.NORMAL


def node_modified_time(node: Node) -> datetime | None:
    """``node.attrs["mtime"]`` narrowed to a real, timezone-aware
    ``datetime`` — ``None`` for a node with no such concept (most
    provider kinds don't set this key at all) or a malformed value.
    Mirrors ``node_file_state``'s own narrow-with-default shape; unlike
    that one there is no meaningful default to fall back to, so this
    returns ``None`` outright rather than inventing a timestamp."""
    value = node.attrs.get("mtime")
    return value if isinstance(value, datetime) else None


def node_leaf_kind(node: Node) -> UnitKind | None:
    """``node.attrs["leaf_kind"]`` narrowed to a real ``UnitKind`` —
    ``None`` for a node with no such concept. Set on every *container*
    node a ``SaasWorkloadProvider``-based provider builds (root and every
    group alike, all sharing the one ``UnitKind`` its
    ``SaasWorkloadConfig.leaf_kind`` declares), so a caller can resolve
    what kind of leaf a given folder holds directly from that folder's
    own ``Node`` — at any depth, with no child inspection needed and no
    ambiguity for a folder with zero children. Unlike ``Node.kind``
    (which a leaf carries directly), this describes a *container's* own
    children's kind, not the node itself."""
    value = node.attrs.get("leaf_kind")
    return value if isinstance(value, UnitKind) else None


def node_kind_label(node: Node) -> str:
    """``node.kind.value``, or a generic ``"folder"``/``"item"`` fallback
    for a ``Node`` whose ``kind`` isn't known (most container nodes, and a
    handful of leaf kinds no provider ever sets) — the one place every
    CLI/TUI render site derives a printable kind label from a resolved
    item-tree ``Node``, so no two of them can disagree on it."""
    if node.kind is not None:
        return node.kind.value
    return "folder" if not node.is_leaf else "item"


def mtime_from_epoch(epoch: int | None) -> datetime | None:
    """The write-side counterpart of ``node_modified_time`` -- converts a
    provider's own raw epoch-seconds catalog value (FS's ``file_mtime``,
    Drive's ``mtime``, ...) into what that accessor reads back. ``None``
    both when ``epoch`` itself is ``None`` (nothing to convert) and when
    it's outside ``datetime``'s own representable range
    (``OverflowError``/``OSError``/``ValueError``) -- a corrupt-catalog
    value must degrade this one node's Modified cell to blank rather
    than failing its whole containing folder's listing."""
    if epoch is None:
        return None
    try:
        return datetime.fromtimestamp(epoch, UTC)
    except (OverflowError, OSError, ValueError):
        return None


def mtime_attr(raw: object) -> datetime | None:
    """``mtime_from_epoch(as_int(raw))``, narrowing a ``Table.select()``
    row's own untyped ``object | None`` value first -- shared by every
    provider whose ``extra_attrs``/``_group_attrs`` populates
    ``attrs["mtime"]`` straight from one raw, optional epoch-seconds
    column, rather than each repeating the same ``as_int``-then-convert
    pair (and its own ``is not None`` guard, needed only because
    ``as_int`` itself raises on ``None`` rather than accepting it)."""
    return mtime_from_epoch(as_int(raw)) if raw is not None else None


def mtime_attrs(raw: object, key: str = "mtime") -> dict[str, object]:
    """``{key: mtime_attr(raw)}``, or ``{}`` when that's ``None`` -- the
    one-line ``attrs.update(...)``/``return`` shape every
    ``extra_attrs``/``group_attrs`` callback wanting a single
    epoch-derived attr reduces to, instead of each spelling out its own
    ``if mtime is not None: ...`` guard around ``mtime_attr`` directly.
    ``key`` defaults to the common case (``"mtime"``); Calendar's own
    ``event_start``/``event_end`` are the one caller needing something
    else."""
    mtime = mtime_attr(raw)
    return {key: mtime} if mtime is not None else {}


@dataclasses.dataclass(frozen=True)
class RestorableUnit(Node):
    """A leaf ``Node`` plus a way to actually read its content.

    ``content`` is already-constructed (cheap for a ``DedupFile`` — it does no
    I/O until first read) rather than a lazy thunk, keeping this a plain
    dataclass instead of needing its own ``__post_init__`` wiring.
    """

    content: ContentSource | None = None

    def open(self) -> ContentSource:
        if self.content is None:
            raise ValueError(f"RestorableUnit {self.name!r} has no content source")
        return self.content


def diagnostic_node(ref: NodeRef, name: str, attrs: dict[str, Any]) -> Node:
    """One synthetic listing node standing in for a provider's own
    degrade-instead-of-fail case (``units/device_pcps.py``'s missing-fids
    node, ``units/device_disk_fs.py``'s no-filesystem-recognized node,
    ...) — the ``Node(is_leaf=True, kind=UnitKind.FILE, ...)`` shape every
    one of them shares; only ``attrs`` (which ``_kind`` marks it as, and
    what ``unit()`` needs to raise the right diagnostic ``NotFoundError``
    later) actually differs between them.
    """
    return Node(ref=ref, name=name, is_leaf=True, kind=UnitKind.FILE, attrs=attrs)


def node_is_diagnostic(node: Node) -> bool:
    """Whether ``node`` is a ``diagnostic_node()`` placeholder rather than
    a real, restorable file — the one check every CLI/TUI render site
    needs so a placeholder isn't shown indistinguishably from real
    content (it looks identical otherwise: same ``kind``, same
    ``is_leaf``)."""
    return "diagnostic" in node.attrs


def not_restorable(kind: str, ref: object) -> NoReturn:
    """Raise the standard ``ValueError`` every ``UnitProvider.unit``
    implementation raises for a resolved-but-contentless node/item.

    Args:
        kind: ``"node"`` or ``"item"``.
        ref: That node's ``name`` or that item's ``key``.
    """
    raise ValueError(f"{kind} {ref!r} is not a restorable unit")


def dir_first_sort_key(is_dir: bool, name: str) -> tuple[int, str]:
    """The child-ordering policy every browsable file/folder-tree
    ``UnitProvider`` applies: containers before leaves, then each group
    alphabetically by name (byte/codepoint comparison, no case folding).
    Use as a ``sorted(..., key=...)`` key for a provider that sorts in
    Python; ``dir_first_order_by`` expresses the same policy as SQL for
    a provider that pushes ordering down into its own query instead."""
    return (0 if is_dir else 1, name)


def dir_first_order_by(is_dir_sql: str, order_by: str) -> str:
    """The same containers-before-leaves-then-name policy as
    ``dir_first_sort_key``, expressed as a SQL ``ORDER BY`` expression.
    ``is_dir_sql`` is a boolean SQL expression true for a container row;
    ``order_by`` is the already-built name(+tiebreaker) clause ranking
    rows within each group."""
    return f"(CASE WHEN {is_dir_sql} THEN 0 ELSE 1 END), {order_by}"


def disk_fs_containers_before_leaves(nodes: list[Node]) -> list[Node]:
    """Stable-partitions an already-ordered node list into containers
    (``is_leaf=False``) before leaves, each group keeping its incoming
    relative order — for a disk-image node interleaved with its own
    "(filesystem)" sibling (``device_disk_fs.DiskFsSibling.root_node``),
    whose pairwise order already comes from something more meaningful
    than name (a disk index, a database ``ORDER BY``), where
    ``dir_first_sort_key``'s alphabetical secondary key would undo it."""
    return sorted(nodes, key=lambda node: node.is_leaf)


def paginate(items: Sequence[_T], offset: int, limit: int | None) -> list[_T]:
    """Slice ``items[offset:offset+limit]`` (open-ended when ``limit`` is
    ``None``) — the same ``offset``/``limit`` contract ``UnitProvider.children``
    takes, for a provider whose own listing is already a small,
    fully-materialized Python sequence (group-level listings, an
    in-memory tree walk) rather than something worth pushing down into a
    real SQL ``LIMIT``/``OFFSET`` — unlike ``units/fs.py``'s ``children()``,
    which pushes ordering and limiting down into SQL instead, a different
    mechanism achieving the same contract. Lives here, not in one provider
    subpackage, so every provider family (``units/file_map_tree.py``,
    ``units/saas/raw_object.py``, ...) can share one implementation rather
    than each hand-rolling this same two-line slice independently."""
    stop = offset + limit if limit is not None else None
    return list(items[offset:stop])


@runtime_checkable
class UnitProvider(Protocol):
    """One workload version's browsable tree.

    ``root`` is sync (every implementation resolves its state up front, in
    an async constructor); ``children`` and ``unit`` are async. This is a
    property of the method, uniform across every implementation, even
    ones that happen to do no I/O in a given body.

    No ``close()`` here: providers that own sqlite sources expose their own
    ``async def close()`` instead, without this Protocol requiring one of
    every implementation.
    """

    def root(self) -> Node: ...

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]: ...

    async def unit(self, node: Node) -> RestorableUnit: ...


@runtime_checkable
class ClosableUnitProvider(UnitProvider, Protocol):
    """A ``UnitProvider`` that owns a real ``SqliteSource``/``aiosqlite``
    connection and must be closed once done.

    Every concrete provider that owns one (``FsProvider``, ``DeviceProvider``,
    ``RawObjectProvider``, ``SaasWorkloadProvider``, ``CompositeSaasProvider``)
    implements this in addition to plain ``UnitProvider``; a provider with
    no such connection (``FileMapTreeProvider``) implements only the
    latter. The ``__aenter__``/``__aexit__`` pair makes ``async with`` the
    ordinary way to hold one, closing it on every exit path including an
    exception; a caller that already holds an instance some other way can
    still just ``await`` ``close`` directly.
    """

    async def close(self) -> None: ...

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


@runtime_checkable
class SupportsDirectRefLookup(Protocol):
    """Implemented only by a provider whose tree addresses every node by a
    single, depth-independent key (Drive's ``item_id`` — see
    ``tree_strategy.RecursiveTree``), for which ``find_node``/
    ``find_path_with_children`` cannot use their usual prefix-guided descent
    (a ``NodeRef``'s ``extra_segments`` doesn't grow with depth for such a
    provider, so no child's ref is ever a *longer* prefix of the target's
    than any other's).

    ``resolve_extra`` looks a node up directly, without visiting any other
    node in the tree. ``parent_of`` walks one step toward the root (``None``
    at the version root) — used only to rebuild the ancestor chain a
    caller like the TUI's goto-ref needs; each ancestor's own children are
    then fetched the ordinary way, one ``UnitProvider.children`` call per
    ancestor."""

    async def resolve_extra(self, extra_segments: tuple[str, ...]) -> Node | None: ...

    async def parent_of(self, node: Node) -> Node | None: ...
