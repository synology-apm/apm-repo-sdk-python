"""Display-name and per-``Node`` field helpers shared by ``ls``/``tree``,
so their outputs can't disagree on the same catalog/workload/version/node.
"""

from __future__ import annotations

import dataclasses
from typing import TypedDict

from synology_apm_repo.sdk import (
    Catalog,
    FileState,
    Node,
    Repository,
    Version,
    Workload,
    disambiguate_catalogs,
    disambiguate_versions,
    disambiguate_workloads,
    node_kind_label,
)
from synology_apm_repo.sdk.presentation import diagnostic_suffix, file_state_suffix, safe


def display_ref(node: Node, fs_path: str) -> str:
    """``node.ref`` with its repo path replaced by the ``fs_path`` the
    command was given, so the printed ref works as a CLI argument from the
    same working directory (``node.ref.repo_path`` is the store-relative
    ``layout.repo_root``)."""
    return str(dataclasses.replace(node.ref, repo_path=fs_path))


async def named_catalogs(repo: Repository) -> list[tuple[str, Catalog]]:
    """Every catalog in ``repo``, paired with its disambiguated display name."""
    catalogs = await repo.catalogs()
    return list(zip(disambiguate_catalogs(catalogs), catalogs, strict=True))


async def named_workloads(catalog: Catalog) -> list[tuple[str, Workload]]:
    """Same idea as ``named_catalogs``, one level down."""
    workloads = await catalog.workloads()
    return list(zip(disambiguate_workloads(workloads), workloads, strict=True))


async def named_versions(catalog: Catalog, workload: Workload) -> list[tuple[str, Version]]:
    """Same idea as ``named_catalogs``, for a workload's versions."""
    versions = await catalog.versions(workload)
    return list(zip(disambiguate_versions(versions), versions, strict=True))


class NodeJsonFields(TypedDict, total=False):
    """The ``--json`` keys ``NodeFields.write_json_fields`` sets; only those
    that apply are present."""

    ref: str
    file_state: str
    diagnostic: bool


@dataclasses.dataclass(frozen=True, slots=True)
class NodeFields:
    """One resolved ``Node``'s printable ``kind``/``ref``/``file_state``/
    ``diagnostic`` fields."""

    kind: str
    ref: str | None
    file_state: FileState
    diagnostic: bool

    def write_json_fields(self, target: NodeJsonFields) -> None:
        """Set ``ref``/``file_state``/``diagnostic`` in a ``--json`` payload,
        each left out when it doesn't apply rather than written as
        ``None``/``False``."""
        if self.ref is not None:
            target["ref"] = self.ref
        if self.file_state is not FileState.NORMAL:
            target["file_state"] = self.file_state.value
        if self.diagnostic:
            target["diagnostic"] = True


def node_suffix(*, ref: str | None, file_state: str, diagnostic: bool) -> str:
    """What a human-readable listing line shows after an entry's name: its
    ref (only present under ``--ref``/``--verbose``), then the file-state and
    diagnostic markers, which show by default as user-facing
    backup-completeness information (ARCHITECTURE.md's Presentation
    section). ``ref`` is content-derived, so it goes through ``safe()``."""
    ref_suffix = f"  [dim]{safe(ref)}[/dim]" if ref is not None else ""
    return f"{ref_suffix}{file_state_suffix(file_state)}{diagnostic_suffix(diagnostic)}"


def node_fields(node: Node, *, show_ref: bool, fs_path: str) -> NodeFields:
    """The ``NodeFields`` of one item-tree ``Node``; ``ref`` only when
    ``show_ref``."""
    return NodeFields(
        kind=node_kind_label(node),
        ref=display_ref(node, fs_path) if show_ref else None,
        file_state=node.file_state,
        diagnostic=node.is_diagnostic,
    )
