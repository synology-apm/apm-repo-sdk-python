"""``synology-apm-repo-cli tree <ref>`` — recursive listing, depth-limited
so a huge item tree (thousands of mail/Drive items) doesn't get dumped in
full by accident.
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import Annotated, assert_never

import typer

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.browse import walked_ref
from synology_apm_repo.cli.consoles import console
from synology_apm_repo.cli.naming import (
    NodeFields,
    NodeJsonFields,
    named_catalogs,
    named_versions,
    named_workloads,
    node_fields,
    node_suffix,
)
from synology_apm_repo.cli.options import KeyOption, ObjectDbIdOption, ProfileOption
from synology_apm_repo.cli.paging import render
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import REF_HELP_TREE, SHOW_REF_HELP, TREE_DEPTH_HELP
from synology_apm_repo.sdk import (
    Catalog,
    CatalogFrame,
    Frame,
    Node,
    NodeFrame,
    Repository,
    RootFrame,
    UnitProvider,
    Workload,
    WorkloadFrame,
)
from synology_apm_repo.sdk.presentation import safe


@dataclasses.dataclass(frozen=True, slots=True)
class TreeEntry:
    name: str
    #: ``"catalog"``/``"workload"``/``"version"``, or an item's
    #: ``node_kind_label`` — the ``kind`` ``ls`` prints for the same entry.
    kind: str
    is_leaf: bool
    size: int | None = None
    #: An item-tree entry's ``naming.node_fields`` — the same fields ``ls``
    #: shows for that node; ``None`` for a catalog/workload/version-level
    #: entry, which has no ``Node`` of its own.
    fields: NodeFields | None = None
    children: list[TreeEntry] = dataclasses.field(default_factory=list)


async def _node_entry(
    node: Node, provider: UnitProvider | None, depth: int, *, show_ref: bool, fs_path: str
) -> TreeEntry:
    # Serial, unlike the catalog-level gathers below: children() reads one
    # shared local aiosqlite connection or an in-memory index, so gathering
    # gains no wall-clock time and multiplies peak memory at item-tree scale.
    children: list[TreeEntry] = []
    if provider is not None and not node.is_leaf and depth > 0:
        children = [
            await _node_entry(child, provider, depth - 1, show_ref=show_ref, fs_path=fs_path)
            for child in await provider.children(node)
        ]
    fields = node_fields(node, show_ref=show_ref, fs_path=fs_path)
    return TreeEntry(
        name=node.name, kind=fields.kind, is_leaf=node.is_leaf, size=node.size, fields=fields, children=children
    )


def _entry_to_json(entry: TreeEntry) -> dict[str, object]:
    """``entry``'s fields as a ``--json`` payload: ``ls --json``'s row keys
    (``name``/``kind``/``size``, and for an item whichever of
    ``ref``/``file_state``/``diagnostic`` apply) plus ``is_leaf`` and
    ``children``."""
    payload: dict[str, object] = {"name": entry.name, "kind": entry.kind, "size": entry.size, "is_leaf": entry.is_leaf}
    if entry.fields is not None:
        node_json: NodeJsonFields = {}
        entry.fields.write_json_fields(node_json)
        payload.update(node_json)
    payload["children"] = [_entry_to_json(child) for child in entry.children]
    return payload


def _render_human(entry: TreeEntry, indent: str = "") -> None:
    marker = "" if entry.is_leaf else "/"
    fields = entry.fields
    suffix = (
        node_suffix(ref=fields.ref, file_state=fields.file_state.value, diagnostic=fields.diagnostic)
        if fields is not None
        else ""
    )
    console.print(f"{indent}{safe(entry.name)}{marker}{suffix}")
    for child in entry.children:
        _render_human(child, indent + "  ")


def _render_human_entries(entries: list[TreeEntry], indent: str = "") -> None:
    for entry in entries:
        _render_human(entry, indent)


async def _version_entries(catalog: Catalog, workload: Workload) -> list[TreeEntry]:
    return [
        TreeEntry(name=name, kind="version", is_leaf=True) for name, _version in await named_versions(catalog, workload)
    ]


async def _workload_entries(catalog: Catalog, *, with_versions: bool) -> list[TreeEntry]:
    named = await named_workloads(catalog)
    if not with_versions:
        return [TreeEntry(name=name, kind="workload", is_leaf=False) for name, _workload in named]
    # Gathered: a catalog's workload count is small and bounded.
    children_lists = await asyncio.gather(*(_version_entries(catalog, workload) for _name, workload in named))
    return [
        TreeEntry(name=name, kind="workload", is_leaf=False, children=children)
        for (name, _workload), children in zip(named, children_lists, strict=True)
    ]


async def _catalog_tree(frame: CatalogFrame | WorkloadFrame, depth: int) -> TreeEntry:
    """The tree rooted at a REF that landed on a catalog or workload, neither
    of which is a ``Node``."""
    match frame:
        case WorkloadFrame(catalog=catalog, workload=workload):
            children = await _version_entries(catalog, workload) if depth >= 1 else []
            return TreeEntry(name=workload.display_name, kind="workload", is_leaf=False, children=children)
        case CatalogFrame(catalog=catalog):
            children = await _workload_entries(catalog, with_versions=depth >= 2) if depth >= 1 else []
            return TreeEntry(name=catalog.display_name, kind="catalog", is_leaf=False, children=children)
        case _:
            assert_never(frame)


async def _root_catalog_entries(repo: Repository, depth: int) -> list[TreeEntry]:
    """The repository root's catalogs, as a bare list with no wrapping
    entry. Shown at any ``--depth``, as ``ls`` shows them; ``depth`` gates
    only the workload and version levels below."""
    named = await named_catalogs(repo)
    if depth < 1:
        return [TreeEntry(name=name, kind="catalog", is_leaf=False) for name, _catalog in named]
    # Gathered, as in _workload_entries.
    children_lists = await asyncio.gather(
        *(_workload_entries(catalog, with_versions=depth >= 2) for _name, catalog in named)
    )
    return [
        TreeEntry(name=name, kind="catalog", is_leaf=False, children=children)
        for (name, _catalog), children in zip(named, children_lists, strict=True)
    ]


async def _result_for_frame(
    repo: Repository, frame: Frame, *, depth: int, show_ref: bool, fs_path: str
) -> TreeEntry | list[TreeEntry]:
    match frame:
        case RootFrame():
            return await _root_catalog_entries(repo, depth)
        case NodeFrame():
            if frame.node.is_leaf:
                return await _node_entry(frame.node, None, 0, show_ref=show_ref, fs_path=fs_path)
            if frame.node == frame.provider.root():
                # The provider's placeholder root (e.g. "Devices"/"Disks"):
                # list its children at top level rather than under it.
                children = await frame.provider.children(frame.node)
                return [
                    await _node_entry(child, frame.provider, depth, show_ref=show_ref, fs_path=fs_path)
                    for child in children
                ]
            return await _node_entry(frame.node, frame.provider, depth, show_ref=show_ref, fs_path=fs_path)
        case CatalogFrame() | WorkloadFrame():
            return await _catalog_tree(frame, depth)
        case _:
            assert_never(frame)


@typer_async
async def tree(
    ctx: typer.Context,
    ref: Annotated[str, typer.Argument(help=REF_HELP_TREE)],
    key: KeyOption = None,
    depth: Annotated[int, typer.Option("--depth", help=TREE_DEPTH_HELP)] = 3,
    show_ref: Annotated[bool, typer.Option("--ref", help=SHOW_REF_HELP)] = False,
    object_db_id: ObjectDbIdOption = None,
    profile: ProfileOption = None,
) -> None:
    """Recursively list everything under REF, up to --depth levels deep."""
    state: CliState = ctx.obj
    async with walked_ref(state, ref, key, profile=profile, object_db_id=object_db_id, show_ref=show_ref) as walked:
        result = await _result_for_frame(
            walked.repo, walked.frame, depth=depth, show_ref=walked.show_ref, fs_path=walked.fs_path
        )

    # --json is always a list of top-level entries, a ref landing on one
    # node included, so a script reads one shape whatever REF names.
    entries = result if isinstance(result, list) else [result]

    def human() -> None:
        if isinstance(result, list):
            _render_human_entries(result)
        else:
            _render_human(result)

    render(console, state, json=[_entry_to_json(entry) for entry in entries], human=human, page=True)
