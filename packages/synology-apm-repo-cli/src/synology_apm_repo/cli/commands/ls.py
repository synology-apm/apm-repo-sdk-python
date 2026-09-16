"""``synology-apm-repo-cli ls <ref>`` — see ``ls()``'s own docstring, shown
via ``--help``, for the ref-navigation model.
"""

from __future__ import annotations

from typing import NotRequired, TypedDict

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
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import REF_HELP_LS, SHOW_REF_HELP
from synology_apm_repo.sdk.api import Catalog, Version, Workload
from synology_apm_repo.sdk.presentation.format import format_bytes
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.base import Node

console = Console()


class _Row(TypedDict):
    """One printed/``--json`` row — ``ref``/``id`` only present when
    ``--ref``/``--verbose`` asked for it (see ``_node_row``/
    ``_catalog_rows``), never a placeholder ``None``. ``ref`` is an
    item-level ``NodeRef``; ``id`` (catalog/workload/version rows, which
    have no standalone ``NodeRef`` of their own) is the bare internal id
    (``catalog_id``/``workload_id``/``version_id``) ``doctor --verbose``
    already surfaces the same way — a plain ``int`` for workload/version,
    but ``catalog_id`` is a string (see ``identifiers.CatalogId``'s own
    docstring for why)."""

    name: str
    kind: str
    size: int | None
    ref: NotRequired[str]
    id: NotRequired[int | str]


def _row(name: str, *, kind: str, size: int | None, ref: str | None, id_: int | str | None = None) -> _Row:
    row: _Row = {"name": name, "kind": kind, "size": size}
    if ref is not None:
        row["ref"] = ref
    if id_ is not None:
        row["id"] = id_
    return row


def _stable_id(obj: object) -> int | str:
    # ``obj`` is really ``Catalog | Workload | Version`` -- typed as
    # plain ``object`` because zip()-ing over the union-of-lists
    # ``_catalog_rows`` takes loses the element type; narrowed back via
    # isinstance immediately below.
    if isinstance(obj, Catalog):
        return str(obj.catalog_id)
    if isinstance(obj, Workload):
        return obj.workload_id
    assert isinstance(obj, Version)
    return obj.version_id


async def _rows_for_node_frame(frame: Frame, *, show_ref: bool, fs_path: str) -> list[_Row]:
    """``ls`` rows once REF has resolved into a version's own item tree
    (``frame.level == "node"``) — ``ls`` on a leaf shows just that one
    item, matching Unix ``ls``'s behavior on a plain file."""
    assert frame.node is not None
    if frame.node.is_leaf or frame.provider is None:
        return [_node_row(frame.node, show_ref=show_ref, fs_path=fs_path)]
    children = await frame.provider.children(frame.node)
    return [_node_row(child, show_ref=show_ref, fs_path=fs_path) for child in children]


def _node_row(node: Node, *, show_ref: bool, fs_path: str) -> _Row:
    return _row(
        node.name,
        kind=node.kind.value if node.kind is not None else ("folder" if not node.is_leaf else "item"),
        size=node.size,
        ref=display_ref(node, fs_path) if show_ref else None,
    )


def _catalog_rows(
    objects: list[Catalog] | list[Workload] | list[Version],
    pairs: list[tuple[str, str]],
    kind: str,
    *,
    hints: list[str | None] | None = None,
    show_ref: bool,
) -> list[_Row]:
    rows: list[_Row] = []
    for name, obj in disambiguated_names(objects, pairs, hints=hints):
        rows.append(_row(name, kind=kind, size=None, ref=None, id_=_stable_id(obj) if show_ref else None))
    return rows


@typer_async
async def ls(
    ctx: typer.Context,
    ref: str = typer.Argument(..., help=REF_HELP_LS),
    key: KeyOption = None,
    show_ref: bool = typer.Option(False, "--ref", help=SHOW_REF_HELP),
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
    parsed = parse_ref_argument(ref)
    effective_show_ref = state.verbose or show_ref
    async with opened_repo(parsed.fs_path, key, profile=profile, state=state) as repo:
        frame = await walk_ref(repo, parsed.node_ref, object_db_id=object_db_id)

        match frame.level:
            case "root":
                catalogs = await repo.catalogs()
                rows = _catalog_rows(catalogs, catalog_pairs(catalogs), "catalog", show_ref=effective_show_ref)
            case "catalog":
                assert frame.catalog is not None
                workloads = await frame.catalog.workloads()
                pairs, hints = workload_pairs(workloads)
                rows = _catalog_rows(workloads, pairs, "workload", hints=hints, show_ref=effective_show_ref)
            case "workload":
                assert frame.catalog is not None and frame.workload is not None
                versions = await frame.catalog.versions(frame.workload)
                rows = _catalog_rows(versions, version_pairs(versions), "version", show_ref=effective_show_ref)
            case _:
                rows = await _rows_for_node_frame(frame, show_ref=effective_show_ref, fs_path=parsed.fs_path)

    if state.json:
        console.print_json(data=rows)
        return
    if not rows:
        console.print("[dim](empty)[/dim]")
        return
    for row in rows:
        # Human-mode display only -- state.json's own ``rows`` above keep
        # the raw int, matching every other size the CLI/TUI print via
        # this same SDK-shared formatter (export.py's summary line,
        # progress_render.py's rate/bar rendering). Content-derived values
        # are escaped at interpolation time (safe()) -- see its own
        # docstring for why.
        size = f" ({format_bytes(row['size'])})" if row["size"] is not None else ""
        id_suffix = f"  [dim]id={row['id']}[/dim]" if "id" in row else ""
        ref_suffix = f"  [dim]{safe(row['ref'])}[/dim]" if "ref" in row else ""
        console.print(f"{safe(row['name'])}{size}{id_suffix}{ref_suffix}")
