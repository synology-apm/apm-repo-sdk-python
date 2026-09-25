"""Display-name/kind helpers shared by ``ls``/``tree`` — the "fetch, build
collision-suffix pairs, disambiguate" and "derive a printable per-``Node``
field set" sequences both commands need so their outputs can't silently
disagree on the same catalog/workload/version/node.
"""

from __future__ import annotations

import dataclasses

from synology_apm_repo.sdk.api import Catalog, Repository, Version, Workload
from synology_apm_repo.sdk.units.base import FileState, Node, node_file_state, node_is_diagnostic, node_kind_label
from synology_apm_repo.sdk.units.node_ref import disambiguate_catalogs, disambiguate_versions, disambiguate_workloads


def display_ref(node: Node, fs_path: str) -> str:
    """The printed ref must be directly reusable, as printed, as a fresh
    CLI argument from the same working directory.

    ``node.ref.repo_path`` is the SDK's own ``layout.repo_root`` (a
    short, store-relative fragment like ``""`` or
    ``"@ActiveProtectVault"``), never the filesystem path REF was
    actually invoked with, so printing ``str(node.ref)`` verbatim would
    hand back something that only round-trips by coincidence. Substitute
    the real ``fs_path`` this command was given before printing. Shared
    by ``ls``/``tree`` — both print refs from the same kind of resolved
    ``Node``."""
    return str(dataclasses.replace(node.ref, repo_path=fs_path))


async def named_catalogs(repo: Repository) -> list[tuple[str, Catalog]]:
    """Every catalog in ``repo``, each paired with its own disambiguated
    display name — the "fetch, disambiguate, zip with the objects those
    names were built from" sequence ``ls``'s own root case and ``tree``'s
    own root-catalog-entries case each repeated independently before this
    helper existed."""
    catalogs = await repo.catalogs()
    return list(zip(disambiguate_catalogs(catalogs), catalogs, strict=True))


async def named_workloads(catalog: Catalog) -> list[tuple[str, Workload]]:
    """Same idea as ``named_catalogs``, one level down."""
    workloads = await catalog.workloads()
    return list(zip(disambiguate_workloads(workloads), workloads, strict=True))


async def named_versions(catalog: Catalog, workload: Workload) -> list[tuple[str, Version]]:
    """Same idea as ``named_catalogs``, one level down from
    ``named_workloads``."""
    versions = await catalog.versions(workload)
    return list(zip(disambiguate_versions(versions), versions, strict=True))


@dataclasses.dataclass(frozen=True)
class NodeFields:
    """One resolved ``Node``'s printable ``kind``/``ref``/``file_state``/
    ``diagnostic`` fields, shared by ``ls``'s row-builder and ``tree``'s
    entry-builders."""

    kind: str
    ref: str | None
    file_state: FileState
    diagnostic: bool


def node_fields(node: Node, *, show_ref: bool, fs_path: str) -> NodeFields:
    """``kind``/``ref``/``file_state``/``diagnostic`` for one resolved
    item-tree ``Node`` — the field set ``ls``'s own row-builder and
    ``tree``'s own entry-builders each derived independently before this
    helper existed, which is what let their ``--json`` output disagree on
    the same node's fields."""
    return NodeFields(
        kind=node_kind_label(node),
        ref=display_ref(node, fs_path) if show_ref else None,
        file_state=node_file_state(node),
        diagnostic=node_is_diagnostic(node),
    )
