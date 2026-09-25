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
    *segment* from ``repo_root``, at any nesting depth — a user pointing
    this tool at their own directory shouldn't see the internal vault/
    object-store naming convention as part of *their* path. Any real
    segment on either side of a marker survives (an object-store repository
    id, a real subdirectory name for a multi-repository scan), since either
    meaningfully distinguishes sibling repositories; the common single-vault
    case reduces to ``""``."""
    # Filtered by *segment*, not a leading str.startswith() prefix check, so
    # this works at any nesting depth: a directory holding multiple repositories one
    # level down (synology-apm-repo-browser samples/, holding apv-sample-1/@ActiveProtectVault,
    # ...) makes repo_root itself start with the real subdirectory name, and a
    # prefix check would match nothing there and leak the marker straight
    # through.
    segments = [s for s in repo_root.split("/") if s not in _INTERNAL_MARKER_PREFIXES]
    return "/".join(segments)


def _repo_path_component(layout: RepositoryLayout, scan_path: str) -> str:
    """The *scanned directory's own name* (what the user actually
    typed — ``layout.repo_root`` on its own is an internal,
    store-relative fragment that means nothing to them) plus whatever
    residual part of ``repo_root`` survives ``_strip_internal_marker`` (only
    present at all for the multi-repository object-store case). Shared by
    ``_repo_label`` (which adds the key-status/diagnostic suffix) and the
    breadcrumb (which doesn't need either). Takes ``layout`` directly,
    not a ``Repository`` -- a real ``Repository`` owns a closable
    connection and can't live in ``core/browse/model.py``'s own frozen
    ``BrowseModel``, only its own ``RepoState`` snapshot of these two
    cheap, sync properties."""
    base = Path(scan_path).name or scan_path or "(current directory)"
    residual = _strip_internal_marker(layout.repo_root)
    return f"{base}/{residual}" if residual else base


def _repo_label(layout: RepositoryLayout, key_status: KeyStatus, scan_path: str, *, verbose: bool) -> str:
    """A column-1 repository label, carrying no internal ids in normal
    mode. The path half is ``_repo_path_component``'s own scanned-directory
    name plus any residual multi-repository marker text. A key-status hint (``· LABEL``, matching ``doctor``'s
    own subtitle style) is appended only once it's actually known. The
    layout-kind suffix is appended only in verbose mode —
    ``uuid``/``catalog_id`` are shown per-catalog instead (see
    ``Catalog.info``), not at this whole-repository label. ``_repo_path_
    component``'s own return value is real, user-typed filesystem-path
    text -- escaped here at the call site (shared with the breadcrumb,
    which escapes at its own call site too): unescaped, dynamic text like
    this can crash a TUI widget on an unmatched or unresolvable markup tag,
    or make the CLI silently swallow a bracketed suffix."""
    label = safe(_repo_path_component(layout, scan_path))
    key_hint = _KEY_STATUS_LABELS.get(key_status)
    if key_hint is not None:
        label += f" · {key_hint}"
    if verbose:
        label += f" (layout: {layout.kind.value})"
    return label


def _catalog_label(catalog: Catalog, name: str, *, verbose: bool) -> str:
    """A column-1 catalog-leaf label — ``name`` is the already-computed,
    disambiguated display name (``catalog_pairs``/``disambiguate``); this
    only adds the verbose-mode suffix, the per-catalog counterpart to
    ``_repo_label``'s own ``layout:`` one — neither ``uuid`` nor
    ``connection_id`` is meaningful at the whole-repository level, so both
    are shown here instead. ``id:`` is ``connection.connection_id``, not
    ``catalog_id`` (see ``sdk.identifiers``' ``CatalogId``/``ConnectionId``
    docstrings) — for a vault, ``catalog_id`` collapses to a small integer
    that's easy to misread as identical across sibling catalogs, whereas
    ``connection_id`` is always a distinct opaque per-catalog string,
    vault or not. ``name``/``uuid``/``connection_id`` are all real,
    dynamic content -- escaped before reaching this label's own ``Tree``
    node, since unescaped it can crash the widget on an unmatched or
    unresolvable markup tag."""
    if not verbose:
        return safe(name)
    return f"{safe(name)} (uuid: {safe(catalog.info.uuid)}, id: {safe(catalog.connection.connection_id)})"
