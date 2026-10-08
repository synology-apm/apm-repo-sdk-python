"""Repository and catalog labels for ``BrowseScreen``'s column 1 and
breadcrumb."""

from __future__ import annotations

from pathlib import Path

from synology_apm_repo.sdk import Catalog, KeyStatus, RepositoryLayout
from synology_apm_repo.sdk.presentation import safe


def repo_path_component(layout: RepositoryLayout, scan_path: str) -> str:
    """The scanned directory's name plus ``layout.display_root``, shared by
    ``repo_label`` and the breadcrumb."""
    base = Path(scan_path).name or scan_path or "(current directory)"
    residual = layout.display_root
    return f"{base}/{residual}" if residual else base


def repo_label(layout: RepositoryLayout, key_status: KeyStatus, scan_path: str, *, verbose: bool) -> str:
    """A repository label: the path (escaped, being user-typed), a
    key-status hint, and in verbose mode the layout kind."""
    label = safe(repo_path_component(layout, scan_path))
    label += f" · {key_status.label}"
    if verbose:
        label += f" (layout: {layout.kind.value})"
    return label


def catalog_label(catalog: Catalog, name: str, *, verbose: bool) -> str:
    """A catalog label from its disambiguated ``name``; verbose mode adds
    ``uuid``/``connection_id`` (not ``catalog_id``, a small integer easily
    misread across siblings). Everything is escaped."""
    if not verbose:
        return safe(name)
    return f"{safe(name)} (uuid: {safe(catalog.info.uuid)}, id: {safe(catalog.connection.connection_id)})"
