"""Shared restorable-unit vocabulary. Every workload provider (Device, FS,
SaaS, ...) implements ``UnitProvider`` against these same three types, so
the CLI/TUI never need to know which workload they're looking at.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from pathlib import Path
from types import TracebackType
from typing import Any, NoReturn, Protocol, Self, TypeVar, runtime_checkable

from ..dedup.dedup_file import DEFAULT_STREAM_BLOCK
from .node_ref import NodeRef

_T = TypeVar("_T")


class UnitKind(enum.Enum):
    """Which restorable-unit kind a ``Node``/``RestorableUnit`` is —
    drives the CLI/TUI's default icon/preview routing."""

    DISK_IMAGE = "disk_image"
    # Cosmetic only (TUI icon/preview routing) — a disk-image node with a
    # "(filesystem)" sibling, or a folder inside that sibling's own
    # tree, still browses/exports identically without this; see
    # units/content/disk_fs.py's own module docstring.
    DISK_FILESYSTEM = "disk_filesystem"
    DISK_FILE = "disk_file"
    FILE = "file"
    MAIL = "mail"
    CONTACT = "contact"
    CALENDAR_EVENT = "calendar_event"
    DRIVE_ITEM = "drive_item"
    SITE_ITEM = "site_item"
    RAW_OBJECT = "raw_object"


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
    degrade-instead-of-fail case (``units/device.py``'s ``target.db``-
    unreadable node, ``units/device_pcps.py``'s missing-fids node,
    ``units/device_disk_fs.py``'s no-filesystem-recognized node, ...) —
    the ``Node(is_leaf=True, kind=UnitKind.FILE, ...)`` shape every one
    of them shares; only ``attrs`` (which ``_kind`` marks it as, and
    what ``unit()`` needs to raise the right diagnostic ``NotFoundError``
    later) actually differs between them.
    """
    return Node(ref=ref, name=name, is_leaf=True, kind=UnitKind.FILE, attrs=attrs)


def not_restorable(kind: str, ref: object) -> NoReturn:
    """Raise the standard ``ValueError`` every ``UnitProvider.unit``
    implementation raises for a resolved-but-contentless node/item.

    Args:
        kind: ``"node"`` or ``"item"``.
        ref: That node's ``name`` or that item's ``key``.
    """
    raise ValueError(f"{kind} {ref!r} is not a restorable unit")


def paginate(items: Sequence[_T], offset: int, limit: int | None) -> list[_T]:
    """Slice ``items[offset:offset+limit]`` (open-ended when ``limit`` is
    ``None``) — the same ``offset``/``limit`` contract ``UnitProvider.children``
    takes, for a provider whose own listing is already a small,
    fully-materialized Python sequence (group-level listings, an
    in-memory tree walk) rather than something worth pushing down into a
    real SQL ``LIMIT``/``OFFSET`` (see ``units/fs.py``'s own ``children()`` for a
    provider that *does* push it down instead, a different mechanism
    achieving the same contract). Lives here, not in one provider
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
