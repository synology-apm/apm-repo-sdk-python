"""``FileMapTreeProvider``: a diagnostic fallback browsing axis straight
off ``db/file_map``, reached via ``Repository.file_map_tree()`` and usable
even when catalog metadata is missing or empty. Its child index costs one
full-table scan of ``file_map``, done once per provider.

It exposes internal ``file_map`` paths directly, so it is
**diagnostic-mode only**, never shown in the default end-user tree.
"""

from __future__ import annotations

import dataclasses
from collections import defaultdict
from typing import override

from .._util.closing import AsyncClosing
from ..dedup.repository import DedupRepo
from ..units.provider_kit import not_restorable, paginate
from .base import Node, RestorableUnit, UnitKind
from .node_ref import NodeRef

# For one prefix ("" for the tree root, otherwise a "/"-joined ancestor
# path): the set of immediate subdirectory names, and a name -> full
# file_map ``path`` mapping for immediate leaf rows. A name can appear in
# both halves at once -- a row can also be a path-prefix of a longer row
# -- so these stay separate rather than collapsing into one "name ->
# is_dir" mapping that would drop one of the two nodes.
_ChildIndex = dict[str, tuple[set[str], dict[str, str]]]


@dataclasses.dataclass(frozen=True, slots=True)
class _Prefix:
    """``Node.handle`` of a directory: the ``file_map`` path prefix it lists."""

    prefix: str


@dataclasses.dataclass(frozen=True, slots=True)
class _Path:
    """``Node.handle`` of a leaf: its full ``file_map`` path."""

    path: str


class FileMapTreeProvider(AsyncClosing):
    """``ClosableUnitProvider`` that turns ``db/file_map``'s ``path`` column — treated
    as ``/``-separated segments — directly into a browsable tree, one leaf
    per row. It reads through the repository's own connections, so closing
    only drops its index."""

    def __init__(self, repo: DedupRepo) -> None:
        self._repo = repo
        self._paths: list[str] | None = None
        self._child_index: _ChildIndex | None = None

    async def _all_paths(self) -> list[str]:
        if self._paths is None:
            conn = await self._repo.db("file_map")
            cursor = await conn.execute("SELECT path FROM file_map")
            self._paths = [row[0] for row in await cursor.fetchall()]
        return self._paths

    async def _index(self) -> _ChildIndex:
        """Every ancestor prefix of every ``file_map`` path, built once per
        provider."""
        if self._child_index is None:
            index: _ChildIndex = defaultdict(lambda: (set(), {}))
            for path in await self._all_paths():
                parts = path.split("/")
                for depth in range(len(parts)):
                    prefix = "/".join(parts[:depth])
                    dir_names, leaves = index[prefix]
                    child = parts[depth]
                    if depth == len(parts) - 1:
                        leaves[child] = path
                    else:
                        dir_names.add(child)
            self._child_index = index
        return self._child_index

    @override
    async def close(self) -> None:
        self._paths = None
        self._child_index = None

    def root(self) -> Node:
        """The ``file_map`` root. No I/O."""
        return Node(ref=NodeRef.raw(self._repo.layout.repo_root, ""), name="/", is_leaf=False, handle=_Prefix(""))

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        if not isinstance(node.handle, _Prefix):
            return []
        prefix = node.handle.prefix
        # Normalized to match _index()'s prefix keys.
        prefix_parts = [p for p in prefix.split("/") if p]
        normalized_prefix = "/".join(prefix_parts)

        dir_names, leaves = (await self._index()).get(normalized_prefix, (set(), {}))

        nodes = []
        for dirname in sorted(dir_names):
            child_prefix = "/".join([*prefix_parts, dirname])
            nodes.append(
                Node(
                    ref=NodeRef.raw(self._repo.layout.repo_root, child_prefix),
                    name=dirname,
                    is_leaf=False,
                    handle=_Prefix(child_prefix),
                )
            )
        for basename, full_path in sorted(leaves.items()):
            nodes.append(
                Node(
                    ref=NodeRef.raw(self._repo.layout.repo_root, full_path),
                    name=basename,
                    is_leaf=True,
                    kind=UnitKind.RAW_OBJECT,
                    details={"path": full_path},
                    handle=_Path(full_path),
                )
            )
        return paginate(nodes, offset, limit)

    async def unit(self, node: Node) -> RestorableUnit:
        if not isinstance(node.handle, _Path):
            not_restorable("node", node.name)
        content = await self._repo.open_file(node.handle.path)
        return RestorableUnit.of(node, content, kind=UnitKind.RAW_OBJECT, size=content.size)
