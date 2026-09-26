"""``synology-apm-repo-cli tree <ref>`` — recursive listing, depth-limited
so a huge item tree (thousands of mail/Drive items) doesn't get dumped in
full by accident.
"""

from __future__ import annotations

import asyncio
import dataclasses

import typer
from rich.console import Console

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.browse import Frame, parse_ref_argument, walk_ref
from synology_apm_repo.cli.naming import named_catalogs, named_versions, named_workloads, node_fields
from synology_apm_repo.cli.options import KeyOption, ObjectDbIdOption, ProfileOption
from synology_apm_repo.cli.paging import render
from synology_apm_repo.cli.repo_session import opened_repo
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import REF_HELP_TREE, SHOW_REF_HELP, TREE_DEPTH_HELP
from synology_apm_repo.sdk.api import Catalog, Repository, Workload
from synology_apm_repo.sdk.presentation.icons import diagnostic_suffix, file_state_suffix
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.base import FileState, Node, UnitProvider

console = Console()


@dataclasses.dataclass(frozen=True)
class TreeEntry:
    name: str
    is_leaf: bool
    #: ``node_fields(node, ...).kind`` for an item-tree entry (``_node_entry``,
    #: the single-leaf-ref branch in ``tree()``) — ``""`` for a catalog/
    #: workload/version-level entry, which has no ``Node`` of its own to
    #: derive one from. Matches ``ls --json``'s own ``kind`` field for
    #: the same node -- both come from the one shared ``naming.node_fields``
    #: helper.
    kind: str = ""
    ref: str | None = None
    #: ``Node.attrs["file_state"]``'s own ``.value`` — see
    #: ``presentation.icons.FILE_STATE_ICON`` for the boundary contract.
    file_state: str = FileState.NORMAL.value
    #: Whether this node is a ``diagnostic_node()`` placeholder rather
    #: than real content — see ``units.base.node_is_diagnostic``.
    diagnostic: bool = False
    children: list[TreeEntry] = dataclasses.field(default_factory=list)


async def _node_entry(node: Node, provider: UnitProvider, depth: int, *, show_ref: bool, fs_path: str) -> TreeEntry:
    # Deliberately serial, not gathered, unlike _workload_entries/
    # _root_catalog_entries above: every UnitProvider.children()
    # implementation bottoms out on one shared local aiosqlite connection
    # or an in-memory index, never per-child network I/O, so gathering
    # here measured zero wall-clock gain while using 3500-4700x more peak
    # memory at the item-tree scale --depth exists to bound.
    children: list[TreeEntry] = []
    if not node.is_leaf and depth > 0:
        children = [
            await _node_entry(child, provider, depth - 1, show_ref=show_ref, fs_path=fs_path)
            for child in await provider.children(node)
        ]
    fields = node_fields(node, show_ref=show_ref, fs_path=fs_path)
    return TreeEntry(
        name=node.name,
        is_leaf=node.is_leaf,
        kind=fields.kind,
        ref=fields.ref,
        file_state=fields.file_state.value,
        diagnostic=fields.diagnostic,
        children=children,
    )


def _entry_to_json(entry: TreeEntry) -> dict[str, object]:
    """``entry``'s fields as a ``--json`` payload, omitting ``kind``/``ref``/
    ``file_state``/``diagnostic`` when they carry their catalog/workload/
    version-level default rather than a real item-tree value — the same
    omit-when-inapplicable convention as ``ls --json``'s ``_row()``."""
    payload: dict[str, object] = {"name": entry.name, "is_leaf": entry.is_leaf}
    if entry.kind:
        payload["kind"] = entry.kind
    if entry.ref is not None:
        payload["ref"] = entry.ref
    if entry.file_state != FileState.NORMAL.value:
        payload["file_state"] = entry.file_state
    if entry.diagnostic:
        payload["diagnostic"] = True
    payload["children"] = [_entry_to_json(child) for child in entry.children]
    return payload


def _render_human(entry: TreeEntry, indent: str = "") -> None:
    marker = "" if entry.is_leaf else "/"
    ref_suffix = f"  [dim]{safe(entry.ref)}[/dim]" if entry.ref is not None else ""
    # file_state/diagnostic markers show in the default view (unlike
    # ref_suffix's internal identifier) since they're user-facing
    # backup-completeness information (ARCHITECTURE.md's Presentation
    # section), not gated behind --verbose.
    state_suffix = file_state_suffix(entry.file_state)
    diag_suffix = diagnostic_suffix(entry.diagnostic)
    console.print(f"{indent}{safe(entry.name)}{marker}{ref_suffix}{state_suffix}{diag_suffix}")
    for child in entry.children:
        _render_human(child, indent + "  ")


def _render_human_entries(entries: list[TreeEntry], indent: str = "") -> None:
    for entry in entries:
        _render_human(entry, indent)


async def _version_entries(catalog: Catalog, workload: Workload) -> list[TreeEntry]:
    return [TreeEntry(name=name, is_leaf=True) for name, _version in await named_versions(catalog, workload)]


async def _workload_entries(catalog: Catalog, *, with_versions: bool) -> list[TreeEntry]:
    named = await named_workloads(catalog)
    if not with_versions:
        return [TreeEntry(name=name, is_leaf=False) for name, _workload in named]
    # Concurrent, not serial -- a catalog's own workload count is a small,
    # bounded sibling collection, the same class doctor.py's own
    # per-catalog gather covers, never item-tree scale.
    children_lists = await asyncio.gather(*(_version_entries(catalog, workload) for _name, workload in named))
    return [
        TreeEntry(name=name, is_leaf=False, children=children)
        for (name, _workload), children in zip(named, children_lists, strict=True)
    ]


async def _catalog_tree(frame: Frame, depth: int) -> TreeEntry:
    """The catalog-level tree for a REF that landed at "catalog" or
    "workload" — neither level is a ``Node``, so this builds its own small
    tree rather than going through ``browse``'s ``Node``-shaped walker.
    Starts wherever ``frame`` landed: pointing REF at a catalog shows that
    catalog's own workloads/versions, not the whole repository again.

    The true bare-root case (``frame.level == "root"``) is handled
    separately by ``_root_catalog_entries``, which returns a bare list
    rather than a single wrapping ``TreeEntry`` — it has no node/catalog/
    workload of its own to name an entry after."""
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
    """The catalog-level entries at a repository's true root, as a bare
    list — not wrapped in a synthetic ``"/"``-named ``TreeEntry``. Always
    shown regardless of ``--depth`` (matching ``ls``); ``depth`` only
    gates how many further levels (workloads, then versions) come with
    them."""
    named = await named_catalogs(repo)
    if depth < 1:
        return [TreeEntry(name=name, is_leaf=False) for name, _catalog in named]
    # Concurrent, not serial -- same bounded-sibling-collection reasoning
    # as _workload_entries above (a repo's own catalog count).
    children_lists = await asyncio.gather(
        *(_workload_entries(catalog, with_versions=depth >= 2) for _name, catalog in named)
    )
    return [
        TreeEntry(name=name, is_leaf=False, children=children)
        for (name, _catalog), children in zip(named, children_lists, strict=True)
    ]


async def _result_for_frame(
    repo: Repository, frame: Frame, *, depth: int, show_ref: bool, fs_path: str
) -> TreeEntry | list[TreeEntry]:
    match frame.level:
        case "root":
            return await _root_catalog_entries(repo, depth)
        case "node":
            assert frame.node is not None
            if frame.node.is_leaf or frame.provider is None:
                fields = node_fields(frame.node, show_ref=show_ref, fs_path=fs_path)
                return TreeEntry(
                    name=frame.node.name,
                    is_leaf=True,
                    kind=fields.kind,
                    ref=fields.ref,
                    file_state=fields.file_state.value,
                    diagnostic=fields.diagnostic,
                )
            if frame.node == frame.provider.root():
                # frame.node is the provider's own un-consumed placeholder
                # root (e.g. device.py's "Devices"/"Disks") — peel one
                # level so its children become the top-level list instead
                # of a fake heading wrapping them.
                children = await frame.provider.children(frame.node)
                return [
                    await _node_entry(child, frame.provider, depth, show_ref=show_ref, fs_path=fs_path)
                    for child in children
                ]
            return await _node_entry(frame.node, frame.provider, depth, show_ref=show_ref, fs_path=fs_path)
        case _:
            return await _catalog_tree(frame, depth)


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
    async with opened_repo(parsed.fs_path, key, profile=profile, state=state) as repo:
        frame = await walk_ref(repo, parsed.node_ref, object_db_id=object_db_id)
        result = await _result_for_frame(repo, frame, depth=depth, show_ref=effective_show_ref, fs_path=parsed.fs_path)

    if isinstance(result, list):
        json_payload: object = [_entry_to_json(entry) for entry in result]

        def human() -> None:
            _render_human_entries(result)
    else:
        json_payload = _entry_to_json(result)

        def human() -> None:
            _render_human(result)

    render(console, state, json=json_payload, human=human, page=True)
