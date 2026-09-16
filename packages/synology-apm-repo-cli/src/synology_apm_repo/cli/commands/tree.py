"""``synology-apm-repo-cli tree <ref>`` — recursive listing, depth-limited
so a huge item tree (thousands of mail/Drive items) doesn't get dumped in
full by accident.
"""

from __future__ import annotations

import dataclasses

import typer
from rich.console import Console

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.browse import (
    Frame,
    catalog_pairs,
    disambiguated_names,
    display_ref,
    opened_repo,
    parse_ref_argument,
    version_pairs,
    walk_ref,
    workload_pairs,
)
from synology_apm_repo.cli.options import KeyOption, ObjectDbIdOption, ProfileOption
from synology_apm_repo.cli.paging import paged
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import REF_HELP_TREE, SHOW_REF_HELP, TREE_DEPTH_HELP
from synology_apm_repo.sdk.api import Catalog, Repository, Workload
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.base import Node, UnitProvider
from synology_apm_repo.sdk.units.node_ref import disambiguate

console = Console()


@dataclasses.dataclass(frozen=True)
class TreeEntry:
    name: str
    is_leaf: bool
    ref: str | None = None
    children: list[TreeEntry] = dataclasses.field(default_factory=list)


async def _node_entry(node: Node, provider: UnitProvider, depth: int, *, show_ref: bool, fs_path: str) -> TreeEntry:
    children: list[TreeEntry] = []
    if not node.is_leaf and depth > 0:
        children = [
            await _node_entry(child, provider, depth - 1, show_ref=show_ref, fs_path=fs_path)
            for child in await provider.children(node)
        ]
    return TreeEntry(
        name=node.name, is_leaf=node.is_leaf, ref=display_ref(node, fs_path) if show_ref else None, children=children
    )


def _render_json(entry: TreeEntry) -> None:
    console.print_json(data=dataclasses.asdict(entry))


def _render_json_entries(entries: list[TreeEntry]) -> None:
    console.print_json(data=[dataclasses.asdict(entry) for entry in entries])


def _render_human(entry: TreeEntry, indent: str = "") -> None:
    marker = "" if entry.is_leaf else "/"
    ref_suffix = f"  [dim]{safe(entry.ref)}[/dim]" if entry.ref is not None else ""
    console.print(f"{indent}{safe(entry.name)}{marker}{ref_suffix}")
    for child in entry.children:
        _render_human(child, indent + "  ")


def _render_human_entries(entries: list[TreeEntry], indent: str = "") -> None:
    for entry in entries:
        _render_human(entry, indent)


async def _version_entries(catalog: Catalog, workload: Workload) -> list[TreeEntry]:
    versions = await catalog.versions(workload)
    names = disambiguate(version_pairs(versions))
    return [TreeEntry(name=name, is_leaf=True) for name in names]


async def _workload_entries(catalog: Catalog, *, with_versions: bool) -> list[TreeEntry]:
    workloads = await catalog.workloads()
    pairs, hints = workload_pairs(workloads)
    return [
        TreeEntry(name=name, is_leaf=False, children=await _version_entries(catalog, workload) if with_versions else [])
        for name, workload in disambiguated_names(workloads, pairs, hints=hints)
    ]


async def _catalog_tree(frame: Frame, depth: int) -> TreeEntry:
    """The catalog-level tree for a REF that landed at "catalog" or
    "workload" — neither level is a ``Node``, so this builds its own
    small tree rather than going through ``browse``'s ``Node``-shaped
    walker. Starts wherever ``frame`` landed: pointing REF at a catalog
    shows that catalog's own workloads/versions, not the whole repository
    from the top again. Every level's names run through the same
    collision-suffix ``disambiguate`` scheme ``ls``/``walk()`` use.

    The true bare-root case (``frame.level == "root"``) is handled
    separately by ``_root_catalog_entries`` — it has no real node/catalog/
    workload of its own to name an entry after, so unlike this function's
    two cases it returns a bare list, not a single wrapping ``TreeEntry``
    (see ``tree()``'s own docstring/comments for why)."""
    match frame.level:
        case "workload":
            assert frame.catalog is not None and frame.workload is not None
            children = await _version_entries(frame.catalog, frame.workload) if depth >= 1 else []
            return TreeEntry(name=frame.workload.display_name, is_leaf=False, children=children)
        case "catalog":
            assert frame.catalog is not None
            children = await _workload_entries(frame.catalog, with_versions=depth >= 2) if depth >= 1 else []
            return TreeEntry(name=frame.catalog.display_name, is_leaf=False, children=children)
        case _:
            raise AssertionError(f"_catalog_tree called with unexpected frame.level: {frame.level!r}")


async def _root_catalog_entries(repo: Repository, depth: int) -> list[TreeEntry]:
    """The catalog-level entries at a repository's true root, as a bare list --
    not wrapped in a synthetic ``"/"``-named ``TreeEntry``. The top-level
    list itself is always shown (matching ``ls``'s own behavior at the
    same level, and this function's sibling ``_catalog_tree``'s
    "catalog"/"workload" cases, which likewise always show themselves
    regardless of ``--depth``) — ``depth`` only gates how many further
    levels (workloads, then versions) come with them."""
    catalogs = await repo.catalogs()
    return [
        TreeEntry(
            name=name,
            is_leaf=False,
            children=await _workload_entries(catalog, with_versions=depth >= 2) if depth >= 1 else [],
        )
        for name, catalog in disambiguated_names(catalogs, catalog_pairs(catalogs))
    ]


@typer_async
async def tree(
    ctx: typer.Context,
    ref: str = typer.Argument(..., help=REF_HELP_TREE),
    key: KeyOption = None,
    depth: int = typer.Option(3, "--depth", help=TREE_DEPTH_HELP),
    show_ref: bool = typer.Option(False, "--ref", help=SHOW_REF_HELP),
    object_db_id: ObjectDbIdOption = None,
    profile: ProfileOption = None,
) -> None:
    """Recursively list everything under REF, up to --depth levels deep."""
    state: CliState = ctx.obj
    parsed = parse_ref_argument(ref)
    effective_show_ref = state.verbose or show_ref
    result: TreeEntry | list[TreeEntry]
    async with opened_repo(parsed.fs_path, key, profile=profile, state=state) as repo:
        frame = await walk_ref(repo, parsed.node_ref, object_db_id=object_db_id)
        if frame.level == "root":
            result = await _root_catalog_entries(repo, depth)
        elif frame.level != "node":
            result = await _catalog_tree(frame, depth)
        else:
            assert frame.node is not None
            if frame.node.is_leaf or frame.provider is None:
                result = TreeEntry(
                    name=frame.node.name,
                    is_leaf=True,
                    ref=(display_ref(frame.node, parsed.fs_path) if effective_show_ref else None),
                )
            elif frame.node == frame.provider.root():
                # frame.node is the provider's own un-consumed root -- a
                # placeholder label (e.g. device.py's "Devices"/"Disks",
                # fs.py's "/") the user never typed and walk_human_ref
                # never matched against a real segment, so (like the
                # bare-root case above) it must not print as if it were
                # an ordinary, addressable child. root() is documented
                # pure/no-I/O on every provider, so calling it again here
                # to compare is free. Peel one level: the root's own
                # children become the top-level list (always shown, same
                # depth budget _node_entry would give them one level
                # down), instead of a fake heading wrapping them.
                children = await frame.provider.children(frame.node)
                result = [
                    await _node_entry(child, frame.provider, depth, show_ref=effective_show_ref, fs_path=parsed.fs_path)
                    for child in children
                ]
            else:
                result = await _node_entry(
                    frame.node, frame.provider, depth, show_ref=effective_show_ref, fs_path=parsed.fs_path
                )

    if state.json:
        if isinstance(result, list):
            _render_json_entries(result)
        else:
            _render_json(result)
    else:
        with paged(console):
            if isinstance(result, list):
                _render_human_entries(result)
            else:
                _render_human(result)
