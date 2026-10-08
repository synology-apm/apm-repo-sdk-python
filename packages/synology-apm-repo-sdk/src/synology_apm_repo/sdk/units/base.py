"""Shared restorable-unit vocabulary. Every workload provider (Device, FS,
SaaS, ...) implements ``UnitProvider`` against these same three types, so
the CLI/TUI never need to know which workload they're looking at.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import AsyncIterator, Mapping
from datetime import datetime
from types import TracebackType
from typing import Any, Protocol, Self, runtime_checkable

from ..dedup.dedup_file import DEFAULT_STREAM_BLOCK
from ..dedup.export_scheduler import ExportTuning
from ..dedup.export_sink import ExportWriter, WrittenBytesCallback
from ..dedup.extent import ExportResult
from .node_ref import NodeRef


class UnitKind(enum.Enum):
    """Which restorable-unit kind a ``Node``/``RestorableUnit`` is —
    drives the CLI/TUI's default icon/preview routing."""

    DISK_IMAGE = "disk_image"
    # The "(filesystem)" sibling of a disk image and each partition/folder
    # inside it; a tree export skips these containers (``plan_tree_export``),
    # since their files are the disk image's own bytes.
    DISK_FILESYSTEM = "disk_filesystem"
    DISK_FILE = "disk_file"
    FILE = "file"
    MAIL = "mail"
    CONTACT = "contact"
    CALENDAR_EVENT = "calendar_event"
    DRIVE_ITEM = "drive_item"
    SITE_ITEM = "site_item"
    RAW_OBJECT = "raw_object"
    #: A Teams channel or Chat conversation's rendered transcript page —
    #: both a leaf's own kind and every ``TeamsChatProvider`` container's
    #: ``leaf_kind`` (root, each channel category).
    TEAMS_CHAT_MESSAGE = "teams_chat_message"
    #: A container whose children are always further containers, never a
    #: leaf — a SharePoint site's "List" category and a Calendar's
    #: "My"/"Other Calendars" category.
    CATEGORY_GROUP = "category_group"


class FileState(enum.Enum):
    """A disk-fs file's cloud-sync/encryption state, carried as
    ``Node.file_state`` — produced other than ``NORMAL`` only by ``units/content/
    disk_fs/``'s NTFS/APFS backends. ``CLOUD_ONLY`` always wins over
    ``ENCRYPTED`` when both apply: an evicted cloud placeholder has no
    local bytes to export regardless of EFS encryption."""

    NORMAL = "normal"
    ENCRYPTED = "encrypted"
    CLOUD_ONLY = "cloud_only"


@runtime_checkable
class ContentSource(Protocol):
    """The one content-reading contract every ``RestorableUnit.content``
    satisfies — a ``DedupFile``/``ByteRangeView`` satisfies this shape already,
    as does assembled content (``.eml``, ``.ics``, ...), so CLI/TUI never
    need to tell them apart."""

    @property
    def size(self) -> int | None:
        """Synchronous; ``None`` when the size isn't known up front (e.g.
        ``LazyArtifact`` before its artifact is assembled). A ``@property``
        because Protocol attributes are invariant, so a plain field would
        reject a narrower ``int``."""
        ...

    async def read(self, offset: int = 0, length: int | None = None) -> bytes | bytearray:
        """Read ``length`` bytes starting at ``offset`` (default: from
        ``offset`` to the end). A request extending past the content's own
        end — including ``offset`` starting at or past it — returns only
        the bytes that exist (possibly none), never an error. A
        ``bytearray`` result belongs to the caller (dedup-backed content returns the buffer it filled
        rather than copying it into ``bytes``).

        Raises:
            ValueError: ``offset`` or ``length`` is negative.
            ResourceLimitExceededError: For a ``DedupFile``-backed
                implementation, the resolved read size exceeds
                ``MAX_SINGLE_READ_SIZE`` (1 GiB); use ``stream()`` for a
                large read.
        """
        ...

    def stream(self, block: int = DEFAULT_STREAM_BLOCK) -> AsyncIterator[tuple[int, bytes | bytearray]]:
        """Yield ``(offset, data)`` blocks of at most ``block`` bytes that
        cover the content in order."""
        ...

    async def export_range(
        self,
        writer: ExportWriter,
        start: int,
        end: int,
        /,
        *,
        sparse: bool = True,
        progress: WrittenBytesCallback | None = None,
        tuning: ExportTuning | None = None,
    ) -> ExportResult:
        """Writes the content's ``[start, end)`` into ``writer``, which the
        caller has already opened and will commit or abort itself
        (``api.run_export`` does all of it), at offsets relative to
        ``start``. ``sparse=True`` may leave ``ZERO``/``HOLE`` ranges
        unwritten. ``progress`` receives each newly written byte count as it
        is handed to ``writer`` (the counts add up to ``planned_bytes`` over
        the range); ``tuning`` is for implementations backed by the dedup
        layer and ignored by the others.

        Raises:
            ValueError: The range is not inside ``[0, size]``."""
        ...

    async def planned_bytes(self, start: int, end: int, /) -> int:
        """The bytes ``export_range`` over ``[start, end)`` reports as
        progress: the real ``DATA`` bytes for dedup-backed content, the
        whole range for content without holes."""
        ...


class NodeRole(enum.Enum):
    """How a frontend presents a container node beyond listing its children."""

    ORDINARY = "ordinary"
    LIST_OVERVIEW = "list_overview"
    """A plain SharePoint List's own group node, shown as a non-expandable
    tree leaf whose items read as one spreadsheet-style overview
    (``read_site_list_items``)."""
    FLAT_CATEGORY = "flat_category"
    """A site's "List" category node, whose Lists are listed as ordinary
    rows but kept out of a folder tree."""


@dataclasses.dataclass(frozen=True, slots=True)
class ItemColumns:
    """The per-kind fields of a listed item a frontend lays out as list
    columns; each is ``None`` where it doesn't apply or isn't known.

    Attributes:
        sender: A mail's sender, as a display name.
        email: A contact's primary email address.
        event_start: A calendar event's start (timezone-aware).
        event_end: A calendar event's end (timezone-aware).
        recurrence: A calendar event's recurrence, as a short label.
    """

    sender: str | None = None
    email: str | None = None
    event_start: datetime | None = None
    event_end: datetime | None = None
    recurrence: str | None = None


@dataclasses.dataclass(frozen=True, slots=True)
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
        mtime: Last-modified time (timezone-aware), when known.
        file_state: A disk-filesystem file's cloud-sync/encryption state.
        leaf_kind: For a container, the kind of the restorable units below
            it, when the provider knows it without listing them.
        diagnostic: Set on a placeholder standing in for content that could
            not be read: the user-facing reason. Such a node is not real
            content (``is_diagnostic``).
        role: How a frontend presents this container.
        degraded: Set on real content that could only be partly resolved:
            a short, user-facing reason. Unlike ``diagnostic``, the node is
            still real content. On a listed node it means the listing is
            partial (a Teams chat shown by its raw id, whose unit leaves it
            unset); on a ``RestorableUnit`` it means the content itself is
            incomplete (a PC/PS disk with missing fragments, known only once
            ``unit()`` assembles it), which an export reports.
        columns: Per-kind fields a frontend lays out as list columns.
        details: Further display-only metadata (path, ids, ...), shown in a
            frontend's verbose view and never read for behaviour.
        export_name: The file name the SDK synthesizes for a leaf with no
            file name of its own in the backup (a mail's subject plus
            ``.eml``); ``None`` when ``name`` already is the file name.
    """

    ref: NodeRef
    name: str
    is_leaf: bool
    kind: UnitKind | None = None
    size: int | None = None
    mtime: datetime | None = None
    file_state: FileState = FileState.NORMAL
    leaf_kind: UnitKind | None = None
    diagnostic: str | None = None
    role: NodeRole = NodeRole.ORDINARY
    degraded: str | None = None
    columns: ItemColumns = dataclasses.field(default_factory=ItemColumns)
    details: Mapping[str, object] = dataclasses.field(default_factory=dict)
    export_name: str | None = None
    handle: object = dataclasses.field(default=None, repr=False, compare=False)
    """What the provider that built this node needs to find its children or
    content again (a tree key, a disk's fragments, ...). Opaque to every
    other caller and never displayed; only that provider reads it."""

    @property
    def is_diagnostic(self) -> bool:
        """Whether this node is a placeholder rather than real content."""
        return self.diagnostic is not None


def node_kind_label(node: Node) -> str:
    """``node.kind.value``, or a generic ``"folder"``/``"item"`` fallback
    for a ``Node`` whose ``kind`` isn't known."""
    if node.kind is not None:
        return node.kind.value
    return "folder" if not node.is_leaf else "item"


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class RestorableUnit(Node):
    """A leaf ``Node`` plus a way to actually read its content.

    ``content`` is already constructed; building one does no I/O until the
    first read.
    """

    content: ContentSource

    @classmethod
    def of(cls, node: Node, content: ContentSource, **changes: Any) -> Self:
        """The unit for leaf ``node``: every ``Node`` field copied (``handle``
        included), then ``changes`` applied, with ``content``."""
        fields = {field.name: getattr(node, field.name) for field in dataclasses.fields(Node)}
        return cls(**{**fields, **changes, "is_leaf": True, "content": content})


@runtime_checkable
class UnitProvider(Protocol):
    """One workload version's browsable tree.

    ``root`` is sync and does no I/O; ``children`` and ``unit`` are async
    in every implementation. Closing is ``ClosableUnitProvider``'s
    contract.
    """

    def root(self) -> Node:
        """The tree's root node."""
        ...

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        """One page of ``node``'s children in a stable order: ``limit``
        entries from ``offset``, or all remaining when ``limit`` is
        ``None``. A leaf has none."""
        ...

    async def unit(self, node: Node) -> RestorableUnit:
        """Open ``node`` as a restorable unit.

        Raises:
            NotRestorableError: ``node`` has no restorable content.
        """


@runtime_checkable
class ClosableUnitProvider(UnitProvider, Protocol):
    """A ``UnitProvider`` that owns open resources (SQLite sources, dedup
    files) and must be closed once done — with ``async with``, or by
    awaiting ``close`` directly.
    """

    async def close(self) -> None:
        """Release what this provider opened."""
        ...

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
    ``find_path_with_children`` cannot use their usual prefix-guided
    descent.

    ``resolve_extra`` looks a node up directly, without visiting any
    other node in the tree (``None`` when absent). ``parent_of`` walks one
    step toward the root (``None`` at the version root), for rebuilding
    the ancestor chain."""

    async def resolve_extra(self, extra_segments: tuple[str, ...]) -> Node | None: ...

    async def parent_of(self, node: Node) -> Node | None: ...
