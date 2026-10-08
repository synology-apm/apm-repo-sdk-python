"""``synology-apm-repo-cli ls <ref>`` — lists REF's children. REF is a
filesystem/store path, optionally followed by ``#name/name/...`` to
navigate into a specific catalog/workload/version/item.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Required, assert_never

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
from synology_apm_repo.cli.strings import REF_HELP_LS, SHOW_REF_HELP
from synology_apm_repo.sdk import Catalog, CatalogFrame, Node, NodeFrame, RootFrame, Version, Workload, WorkloadFrame
from synology_apm_repo.sdk.presentation import format_bytes, safe


class _Row(NodeJsonFields, total=False):
    """One printed/``--json`` row; a key that doesn't apply is omitted, never
    set to ``None``/``False``. ``id`` is the internal id of a
    catalog/workload/version row (which has no ``NodeRef`` of its own), as
    ``doctor --verbose`` shows it: an ``int`` for a workload/version, a
    string ``catalog_id`` for a catalog."""

    name: Required[str]
    kind: Required[str]
    size: Required[int | None]
    id: int | str


def _row(
    name: str, *, kind: str, size: int | None, id_: int | str | None = None, fields: NodeFields | None = None
) -> _Row:
    row = _Row(name=name, kind=kind, size=size)
    if id_ is not None:
        row["id"] = id_
    if fields is not None:
        fields.write_json_fields(row)
    return row


def _stable_id(obj: object) -> int | str:
    # Typed ``object``: ``_catalog_rows``' union-of-lists input loses the
    # element type.
    if isinstance(obj, Catalog):
        return str(obj.catalog_id)
    if isinstance(obj, Workload):
        return obj.workload_id
    assert isinstance(obj, Version)
    return obj.version_id


async def _rows_for_node_frame(frame: NodeFrame, *, show_ref: bool, fs_path: str) -> list[_Row]:
    """``ls`` rows for a REF inside a version's item tree; a leaf lists
    itself, like Unix ``ls`` on a plain file."""
    if frame.node.is_leaf:
        return [_node_row(frame.node, show_ref=show_ref, fs_path=fs_path)]
    children = await frame.provider.children(frame.node)
    return [_node_row(child, show_ref=show_ref, fs_path=fs_path) for child in children]


def _node_row(node: Node, *, show_ref: bool, fs_path: str) -> _Row:
    fields = node_fields(node, show_ref=show_ref, fs_path=fs_path)
    return _row(node.name, kind=fields.kind, size=node.size, fields=fields)


def _catalog_rows(
    named: Sequence[tuple[str, Catalog | Workload | Version]], kind: str, *, show_ref: bool
) -> list[_Row]:
    return [_row(name, kind=kind, size=None, id_=_stable_id(obj) if show_ref else None) for name, obj in named]


def _render_human(rows: list[_Row]) -> None:
    if not rows:
        console.print("[dim](empty)[/dim]")
        return
    for row in rows:
        # name is content-derived, so it goes through safe().
        size = f" ({format_bytes(row['size'])})" if row["size"] is not None else ""
        id_suffix = f"  [dim]id={row['id']}[/dim]" if "id" in row else ""
        suffix = node_suffix(
            ref=row.get("ref"), file_state=row.get("file_state", ""), diagnostic=row.get("diagnostic", False)
        )
        console.print(f"{safe(row['name'])}{size}{id_suffix}{suffix}")


@typer_async
async def ls(
    ctx: typer.Context,
    ref: Annotated[str, typer.Argument(help=REF_HELP_LS)],
    key: KeyOption = None,
    show_ref: Annotated[bool, typer.Option("--ref", help=SHOW_REF_HELP)] = False,
    object_db_id: ObjectDbIdOption = None,
    profile: ProfileOption = None,
) -> None:
    """List REF's children. REF is <path>, optionally followed by
    #<name>/<name>/...: <path> is where the repository lives (a filesystem
    path, or a store-relative path with --profile) and is opened as-is — no
    '#' at all lists the repository's catalogs (backup sources). Add names
    after that single '#', separated by '/', to navigate deeper: the first
    name picks a catalog, the second a workload, the third a version, and
    any further ones go into that version's own item tree. A name
    containing a literal '/' (rare, but real — e.g. a workload named
    '../A') must be percent-encoded within its own segment ('..%2FA') or
    it reads as an extra navigation level instead of part of the name."""
    state: CliState = ctx.obj
    async with walked_ref(state, ref, key, profile=profile, object_db_id=object_db_id, show_ref=show_ref) as walked:
        frame, show = walked.frame, walked.show_ref
        match frame:
            case RootFrame():
                rows = _catalog_rows(await named_catalogs(walked.repo), "catalog", show_ref=show)
            case CatalogFrame(catalog=catalog):
                rows = _catalog_rows(await named_workloads(catalog), "workload", show_ref=show)
            case WorkloadFrame(catalog=catalog, workload=workload):
                rows = _catalog_rows(await named_versions(catalog, workload), "version", show_ref=show)
            case NodeFrame():
                rows = await _rows_for_node_frame(frame, show_ref=show, fs_path=walked.fs_path)
            case _:
                assert_never(frame)

    render(console, state, json=rows, human=lambda: _render_human(rows), page=True)
