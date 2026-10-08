"""Helpers every ``UnitProvider`` implementation builds its nodes with:
catalog-time conversion, diagnostic placeholders, the "not restorable"
error, the dir-first child order and offset/limit pagination."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import NoReturn

from ..errors import NotRestorableError
from ..storage.table import as_int
from .base import Node, UnitKind
from .node_ref import NodeRef


def mtime_from_epoch(epoch: int | None) -> datetime | None:
    """A provider's raw epoch-seconds catalog value as a ``Node.mtime``;
    ``None`` when ``epoch`` is ``None`` or outside ``datetime``'s range."""
    if epoch is None:
        return None
    try:
        return datetime.fromtimestamp(epoch, UTC)
    except (OverflowError, OSError, ValueError):
        return None


def mtime_from_raw(raw: object) -> datetime | None:
    """``mtime_from_epoch`` for an untyped ``Table.select()`` row value."""
    return mtime_from_epoch(as_int(raw)) if raw is not None else None


def diagnostic_node(ref: NodeRef, name: str, diagnostic: str, handle: object = None) -> Node:
    """One synthetic listing node standing in for a provider's own
    degrade-instead-of-fail case, with ``diagnostic`` as its user-facing
    reason."""
    return Node(ref=ref, name=name, is_leaf=True, kind=UnitKind.FILE, diagnostic=diagnostic, handle=handle)


def not_restorable(kind: str, ref: object) -> NoReturn:
    """Raise the ``NotRestorableError`` every ``UnitProvider.unit``
    implementation raises for a resolved-but-contentless node/item.

    Args:
        kind: ``"node"`` or ``"item"``.
        ref: That node's ``name`` or that item's ``key``.
    """
    raise NotRestorableError(f"{kind} {ref!r} is not a restorable unit")


def dir_first_sort_key(is_dir: bool, name: str) -> tuple[int, str]:
    """The child-ordering policy every browsable file/folder-tree
    ``UnitProvider`` applies: containers before leaves, then each group
    alphabetically by name (byte/codepoint comparison, no case folding).
    Use as a ``sorted(..., key=...)`` key for a provider that sorts in
    Python."""
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
    relative order — for a disk-image node interleaved with its
    "(filesystem)" sibling, whose meaningful order (a disk index, a
    database ``ORDER BY``) ``dir_first_sort_key``'s alphabetical key
    would undo."""
    return sorted(nodes, key=lambda node: node.is_leaf)


def paginate[T](items: Sequence[T], offset: int, limit: int | None) -> list[T]:
    """Slice ``items[offset:offset+limit]`` (open-ended when ``limit`` is
    ``None``) — for a provider whose listing is already a small,
    fully-materialized sequence, rather than pushed down into a real SQL
    ``LIMIT``/``OFFSET``."""
    stop = offset + limit if limit is not None else None
    return list(items[offset:stop])
