"""Pure repository-label formatting for ``BrowseScreen``'s column 1 — carry no
``Tree``/widget state at all, so they're directly testable without a
running Textual app. See
``tests/unit/browser/test_browser_browse_screen_labels.py``.
"""

from __future__ import annotations

from pathlib import Path

from synology_apm_repo.sdk.api import Catalog, KeyStatus, RepositoryLayout
from synology_apm_repo.sdk.presentation.markup import safe

#: Internal store-relative marker segments ``layout.repo_root`` may
#: start with — never meaningful to a user, who only ever typed a real
#: filesystem path; stripped before display (see ``_repo_label``).
_INTERNAL_MARKER_PREFIXES = ("@ActiveProtectVault", "@ActiveProtectData")

#: Every ``KeyStatus`` value gets a hint here — none is left unmapped.
_KEY_STATUS_LABELS = {
    KeyStatus.NOT_ENCRYPTED: "not encrypted",
    KeyStatus.NO_KEY_PROVIDED: "key needed",
    KeyStatus.INVALID: "invalid key",
    KeyStatus.VERIFIED: "key verified",
}


def _strip_internal_marker(repo_root: str) -> str:
    """Strips every ``@ActiveProtectVault``/``@ActiveProtectData`` marker
    segment from ``repo_root``, at any nesting depth, so a user doesn't see
    the internal vault/object-store naming convention in their own path.
    Any real segment on either side of a marker survives; the common
    single-vault case reduces to ``""``."""
    segments = [s for s in repo_root.split("/") if s not in _INTERNAL_MARKER_PREFIXES]
    return "/".join(segments)


def _repo_path_component(layout: RepositoryLayout, scan_path: str) -> str:
    """The scanned directory's own name (what the user typed) plus whatever
    residual part of ``repo_root`` survives ``_strip_internal_marker``.
    Shared by ``_repo_label`` and the breadcrumb. Takes ``layout``, not a
    ``Repository``, since a real ``Repository`` owns a closable connection
    and can't live in the frozen ``BrowseModel``."""
    base = Path(scan_path).name or scan_path or "(current directory)"
    residual = _strip_internal_marker(layout.repo_root)
    return f"{base}/{residual}" if residual else base


def _repo_label(layout: RepositoryLayout, key_status: KeyStatus, scan_path: str, *, verbose: bool) -> str:
    """A column-1 repository label with no internal ids in normal mode: the
    path, a key-status hint once known, and (verbose only) the layout kind —
    ``uuid``/``catalog_id`` show per-catalog instead. Escaped at this call
    site since the path is real, user-typed text that can crash a TUI
    widget on an unresolvable markup tag."""
    label = safe(_repo_path_component(layout, scan_path))
    key_hint = _KEY_STATUS_LABELS.get(key_status)
    if key_hint is not None:
        label += f" · {key_hint}"
    if verbose:
        label += f" (layout: {layout.kind.value})"
    return label


def _catalog_label(catalog: Catalog, name: str, *, verbose: bool) -> str:
    """A column-1 catalog-leaf label — ``name`` is the already-disambiguated
    display name; verbose mode adds ``uuid``/``connection_id`` (not
    ``catalog_id``, which collapses to a small integer easy to misread as
    identical across sibling catalogs in a vault). Escaped since all three
    are real, dynamic content."""
    if not verbose:
        return safe(name)
    return f"{safe(name)} (uuid: {safe(catalog.info.uuid)}, id: {safe(catalog.connection.connection_id)})"
