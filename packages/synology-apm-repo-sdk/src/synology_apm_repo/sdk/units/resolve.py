"""Resolving a ``NodeRef`` against a provider's tree without visiting every
node in it — shared by ``Repository.resolve`` (which only needs the final
``Node``) and the TUI's goto-ref feature (which also needs the full
ancestor chain and each ancestor's own children, to repopulate a ``Tree``
widget).

For a provider whose ``extra_segments`` grow by exactly one segment per
tree level (true for every provider except Drive/Team Drive, which key
nodes by a depth-independent id instead), a target's ``extra_segments``
is a checkable prefix relationship: a child is only worth descending
into when its ref is a prefix of the target's. ``find_node``/
``find_path_with_children`` use exactly that to visit only the nodes on
the real path to the target. A provider that can't support this instead
implements ``SupportsDirectRefLookup``, checked via ``isinstance`` before
falling back to descent.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from .base import Node, SupportsDirectRefLookup, UnitProvider
from .node_ref import NodeRef

_PAGE_SIZE = 500
"""Matches the TUI's own children-pagination page size
(``browser/core/unit/update.py``'s ``CHILDREN_PAGE_SIZE``) — large
enough that a real tree level usually resolves in one page, small enough
that a wide level doesn't force one huge fetch before an early match can
short-circuit."""


def _is_strict_prefix(shorter: tuple[str, ...], longer: tuple[str, ...]) -> bool:
    return len(shorter) < len(longer) and longer[: len(shorter)] == shorter


async def _iter_pages(provider: UnitProvider, node: Node) -> AsyncIterator[list[Node]]:
    """Yields ``node``'s children one ``_PAGE_SIZE`` page at a time,
    stopping once a short page proves there's no next one. A consumer
    that ``return``s out of its ``async for`` early simply never
    requests the next page."""
    offset = 0
    while True:
        page = await provider.children(node, offset=offset, limit=_PAGE_SIZE)
        yield page
        if len(page) < _PAGE_SIZE:
            return
        offset += _PAGE_SIZE


async def _children_matching(
    provider: UnitProvider, node: Node, target_extra: tuple[str, ...], *, need_full_list: bool
) -> tuple[Node | None, list[Node], list[Node]]:
    """Fetches ``node``'s children, paginated, returning ``(exact,
    children, candidates)``.

    ``exact`` is the one child (if any) whose ``extra_segments`` equals
    ``target_extra`` — unambiguous, so paginating stops the instant it's
    found.

    ``candidates`` is every non-leaf child whose ``extra_segments`` is a
    strictly shorter prefix of ``target_extra`` — normally at most one,
    but a disk-image node's "(filesystem)" sibling is a real exception:
    its ref extends the image's own ref despite being a tree sibling, not
    a descendant. The caller tries each candidate in turn. Collecting
    every candidate means every page is still fetched when no exact match
    turns up at this level.

    ``children`` (every child fetched so far) is populated only when
    ``need_full_list`` is set — ``find_path_with_children``'s requirement,
    to populate every real sibling for the visible ``Tree`` widget."""
    children: list[Node] = []
    exact: Node | None = None
    candidates: list[Node] = []
    async for page in _iter_pages(provider, node):
        if need_full_list:
            children.extend(page)
        if exact is None:
            for child in page:
                child_extra = child.ref.extra_segments
                if child_extra == target_extra:
                    exact = child
                    break
                if not child.is_leaf and _is_strict_prefix(child_extra, target_extra):
                    candidates.append(child)
            if exact is not None and not need_full_list:
                return exact, children, candidates
    return exact, children, candidates


async def _all_children(provider: UnitProvider, node: Node) -> list[Node]:
    """Every child of ``node``, paginated to exhaustion — used only to
    rebuild an ancestor's full sibling list on the
    ``SupportsDirectRefLookup`` path, where the ancestor is already known
    and no prefix match is needed."""
    children: list[Node] = []
    async for page in _iter_pages(provider, node):
        children.extend(page)
    return children


async def _descend(
    provider: UnitProvider, node: Node, target: NodeRef, *, need_full_list: bool
) -> tuple[Node, list[Node], list[list[Node]]] | None:
    """Returns ``(leaf, chain, children_by_step)`` — ``chain`` is
    ``[node, ..., leaf]``, ``children_by_step[i]`` is ``chain[i]``'s own
    children (populated only when ``need_full_list`` is set; ``[]`` per
    level otherwise) — or ``None`` if ``target`` isn't reachable from
    ``node``."""
    target_extra = target.extra_segments
    if node.ref.extra_segments == target_extra:
        return node, [node], []
    if node.is_leaf:
        return None
    exact, children, candidates = await _children_matching(provider, node, target_extra, need_full_list=need_full_list)
    # An exact match is unambiguous -- try only it. Otherwise try each
    # non-leaf prefix candidate in turn, moving on rather than giving up
    # if an earlier one's subtree doesn't actually contain the target.
    for child in [exact] if exact is not None else candidates:
        deeper = await _descend(provider, child, target, need_full_list=need_full_list)
        if deeper is not None:
            leaf, chain, children_by_step = deeper
            return leaf, [node, *chain], ([children, *children_by_step] if need_full_list else [])
    return None


async def _direct_ancestor_chain(provider: SupportsDirectRefLookup, leaf: Node) -> list[Node]:
    """``[root, ..., leaf]`` for a provider that can't expose ancestry via
    ``extra_segments`` itself — walks ``SupportsDirectRefLookup.parent_of``
    up from ``leaf`` until it returns ``None`` (the version root)."""
    chain = [leaf]
    current = leaf
    while True:
        parent = await provider.parent_of(current)
        if parent is None:
            return list(reversed(chain))
        chain.append(parent)
        current = parent


async def find_node(provider: UnitProvider, target: NodeRef) -> Node | None:
    """The single ``Node`` whose own ``ref`` equals ``target``, or ``None``.
    Visits only the nodes on the real path to ``target`` — never a
    non-matching sibling's subtree."""
    if isinstance(provider, SupportsDirectRefLookup):
        return await provider.resolve_extra(target.extra_segments)
    result = await _descend(provider, provider.root(), target, need_full_list=False)
    return result[0] if result is not None else None


async def find_path_with_children(
    provider: UnitProvider, target: NodeRef
) -> tuple[list[Node], list[list[Node]]] | None:
    """``(chain, children_by_step)`` for ``target`` — ``chain`` is
    ``[root, ..., target]``; ``children_by_step[i]`` is the complete
    (fully paginated) list of ``chain[i]``'s own children, for every
    ancestor up to but not including ``target`` itself. ``None`` if
    ``target`` isn't in this tree at all."""
    if isinstance(provider, SupportsDirectRefLookup):
        node = await provider.resolve_extra(target.extra_segments)
        if node is None:
            return None
        chain = await _direct_ancestor_chain(provider, node)
        children_by_step = [await _all_children(provider, ancestor) for ancestor in chain[:-1]]
        return chain, children_by_step
    result = await _descend(provider, provider.root(), target, need_full_list=True)
    if result is None:
        return None
    _, chain, children_by_step = result
    return chain, children_by_step
