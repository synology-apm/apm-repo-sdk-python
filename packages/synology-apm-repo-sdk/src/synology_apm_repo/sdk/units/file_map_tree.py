"""``FileMapTreeProvider``: a diagnostic fallback browsing axis straight
off ``db/file_map``, reached via ``Repository.file_map_tree()`` and usable
even when catalog metadata is missing or unhelpful — e.g.
``apv-sample-3``'s empty ``copy_meta_file``. Building the child-index (see
``FileMapTreeProvider._index``) costs one full-table scan of ``file_map``,
done once and cached for the provider's lifetime — after that,
``children()`` is a plain dict lookup, not a rescan.

This exposes internal ``file_map`` paths directly and is therefore
**diagnostic-mode only** — never shown in the default, end-user-facing
tree.
"""

from __future__ import annotations

from collections import defaultdict

from ..dedup.repository import DedupRepo
from .base import Node, RestorableUnit, UnitKind, not_restorable, paginate
from .node_ref import NodeRef

# For one prefix ("" for the tree root, otherwise a "/"-joined ancestor
# path): the set of immediate subdirectory names, and a name -> full
# file_map ``path`` mapping for immediate leaf rows. A name can legitimately
# appear in *both* halves at once -- a real file_map row can itself also
# be a path-prefix of a longer row (e.g. an empty-directory object
# alongside a file nested under that same path) -- so this mirrors the
# original per-call ``dir_names``/``leaves`` locals exactly rather than
# collapsing them into one "name -> is_dir" mapping that would silently
# drop one of the two nodes in that case.
_ChildIndex = dict[str, tuple[set[str], dict[str, str]]]


class FileMapTreeProvider:
    """``UnitProvider`` that turns ``db/file_map``'s ``path`` column — treated
    as ``/``-separated segments — directly into a browsable tree, one leaf
    per row."""

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
        """Every ancestor prefix of every ``file_map`` path, indexed once
        in O(row count x average path depth) and amortized across every
        call for this provider's lifetime — ``file_map`` accumulates
        history across every device/VM/PC-PS/SaaS stream ever recorded,
        not just current state, so a real repository's row count can be
        large. ``children()`` itself is then an O(children at this one
        level) dict lookup."""
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

    def root(self) -> Node:
        """Pure construction — no I/O, so this stays synchronous (see
        ``UnitProvider``)."""
        return Node(ref=NodeRef.raw(self._repo.layout.repo_root, ""), name="/", is_leaf=False, attrs={"prefix": ""})

    async def children(self, node: Node, offset: int = 0, limit: int | None = None) -> list[Node]:
        prefix = node.attrs.get("prefix")
        if prefix is None:
            return []
        # Re-normalized from the request's own prefix string (not assumed
        # already-clean), since this lookup key must match exactly how
        # _index() built its own prefix keys ("/".join of filtered,
        # non-empty parts).
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
                    attrs={"prefix": child_prefix},
                )
            )
        for basename, full_path in sorted(leaves.items()):
            nodes.append(
                Node(
                    ref=NodeRef.raw(self._repo.layout.repo_root, full_path),
                    name=basename,
                    is_leaf=True,
                    kind=UnitKind.RAW_OBJECT,
                    attrs={"path": full_path},
                )
            )
        return paginate(nodes, offset, limit)

    async def unit(self, node: Node) -> RestorableUnit:
        path = node.attrs.get("path")
        if path is None:
            not_restorable("node", node.name)
        content = await self._repo.open_file(path)
        return RestorableUnit(
            ref=node.ref,
            name=node.name,
            is_leaf=True,
            kind=UnitKind.RAW_OBJECT,
            size=content.size,
            attrs=node.attrs,
            content=content,
        )
