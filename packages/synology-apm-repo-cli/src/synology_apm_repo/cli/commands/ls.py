"""``synology-apm-repo-cli ls <ref>`` — lists REF's children. REF is a
filesystem/store path, optionally followed by ``#name/name/...`` to
navigate into a specific catalog/workload/version/item.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import NotRequired, TypedDict

import typer
from rich.console import Console

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.browse import Frame, parse_ref_argument, walk_ref
from synology_apm_repo.cli.naming import named_catalogs, named_versions, named_workloads, node_fields
from synology_apm_repo.cli.options import KeyOption, ObjectDbIdOption, ProfileOption
from synology_apm_repo.cli.paging import render
from synology_apm_repo.cli.repo_session import opened_repo
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import REF_HELP_LS, SHOW_REF_HELP
from synology_apm_repo.sdk.api import Catalog, Version, Workload
from synology_apm_repo.sdk.presentation.format import format_bytes
from synology_apm_repo.sdk.presentation.icons import diagnostic_suffix, file_state_suffix
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.units.base import FileState, Node

console = Console()


class _Row(TypedDict):
    """One printed/``--json`` row — ``ref``/``id``/``file_state``/
    ``diagnostic`` are each omitted outright, never set to a bare
    ``False``/``None``, when they don't apply (see
    ``_node_row``/``_catalog_rows``). ``ref`` is an item-level
    ``NodeRef``; ``id`` (catalog/workload/version rows, which have no
    standalone ``NodeRef`` of their own) is the bare internal id
    (``catalog_id``/``workload_id``/``version_id``) ``doctor --verbose``
    already surfaces the same way — a plain ``int`` for workload/version,
    but ``catalog_id`` is a string — an object-storage sibling's own
    numeric id isn't unique repo-wide, so this falls back to the repo-id
    string instead; ``file_state`` is ``FileState.value``, omitted for
    ``FileState.NORMAL`` — see ``presentation.icons.FILE_STATE_ICON`` for
    the boundary contract; ``diagnostic`` is omitted unless ``True`` —
    see ``units.base.node_is_diagnostic``."""

    name: str
    kind: str
    size: int | None
    ref: NotRequired[str]
    id: NotRequired[int | str]
    file_state: NotRequired[str]
    diagnostic: NotRequired[bool]


def _row(
    name: str,
    *,
    kind: str,
    size: int | None,
    ref: str | None,
    id_: int | str | None = None,
    file_state: FileState = FileState.NORMAL,
    diagnostic: bool = False,
) -> _Row:
    row: _Row = {"name": name, "kind": kind, "size": size}
    if ref is not None:
        row["ref"] = ref
    if id_ is not None:
        row["id"] = id_
    if file_state is not FileState.NORMAL:
        row["file_state"] = file_state.value
    if diagnostic:
        row["diagnostic"] = True
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
    fields = node_fields(node, show_ref=show_ref, fs_path=fs_path)
    return _row(
        node.name,
        kind=fields.kind,
        size=node.size,
        ref=fields.ref,
        file_state=fields.file_state,
        diagnostic=fields.diagnostic,
    )


def _catalog_rows(
    named: Sequence[tuple[str, Catalog | Workload | Version]], kind: str, *, show_ref: bool
) -> list[_Row]:
    return [
        _row(name, kind=kind, size=None, ref=None, id_=_stable_id(obj) if show_ref else None) for name, obj in named
    ]


def _render_human(rows: list[_Row]) -> None:
    if not rows:
        console.print("[dim](empty)[/dim]")
        return
    for row in rows:
        # --json keeps row['size'] as a raw int; only this human-mode
        # render formats it, via the same SDK format_bytes used by
        # export.py's summary line and progress_render.py's rate/bar.
        # name/ref are content-derived, so they go through safe() first.
        size = f" ({format_bytes(row['size'])})" if row["size"] is not None else ""
        id_suffix = f"  [dim]id={row['id']}[/dim]" if "id" in row else ""
        ref_suffix = f"  [dim]{safe(row['ref'])}[/dim]" if "ref" in row else ""
        # file_state/diagnostic markers show in the default view (unlike
        # id_suffix/ref_suffix's internal identifiers) since they're
        # user-facing backup-completeness information (ARCHITECTURE.md's
        # Presentation section), not gated behind --verbose. A short
        # emoji marker needs no [dim]/[yellow] tag, unlike id_suffix/
        # ref_suffix's longer text.
        state_suffix = file_state_suffix(row.get("file_state", ""))
        diag_suffix = diagnostic_suffix(row.get("diagnostic", False))
        console.print(f"{safe(row['name'])}{size}{id_suffix}{ref_suffix}{state_suffix}{diag_suffix}")


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
                rows = _catalog_rows(await named_catalogs(repo), "catalog", show_ref=effective_show_ref)
            case "catalog":
                assert frame.catalog is not None
                rows = _catalog_rows(await named_workloads(frame.catalog), "workload", show_ref=effective_show_ref)
            case "workload":
                assert frame.catalog is not None and frame.workload is not None
                rows = _catalog_rows(
                    await named_versions(frame.catalog, frame.workload), "version", show_ref=effective_show_ref
                )
            case _:
                rows = await _rows_for_node_frame(frame, show_ref=effective_show_ref, fs_path=parsed.fs_path)

    render(console, state, json=rows, human=lambda: _render_human(rows), page=True)
