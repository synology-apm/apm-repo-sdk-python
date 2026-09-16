"""Pure repository-label formatting for ``BrowseScreen``'s column 1 — carry no
``Tree``/widget state at all, so they're directly testable without a
running Textual app. See
``tests/unit/browser/test_browser_browse_screen_labels.py``.
"""

from __future__ import annotations

from pathlib import Path

from synology_apm_repo.sdk.api import Catalog, KeyStatus, Repository

#: Internal store-relative marker segments ``layout.repo_root`` may
#: start with — never meaningful to a user, who only ever typed a real
#: filesystem path; stripped before display (see ``_repo_label``).
_INTERNAL_MARKER_PREFIXES = ("@ActiveProtectVault", "@ActiveProtectData")

#: Every ``KeyStatus`` gets a hint — see ``KeyStatus``'s own docstring
#: for what each value guarantees.
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


def _repo_path_component(repo: Repository, scan_path: str) -> str:
    """The *scanned directory's own name* (what the user actually
    typed — ``layout.repo_root`` on its own is an internal,
    store-relative fragment that means nothing to them) plus whatever
    residual part of ``repo_root`` survives ``_strip_internal_marker`` (only
    present at all for the multi-repository object-store case). Shared by
    ``_repo_label`` (which adds the key-status/diagnostic suffix) and the
    breadcrumb (which doesn't need either)."""
    base = Path(scan_path).name or scan_path or "(current directory)"
    residual = _strip_internal_marker(repo.layout.repo_root)
    return f"{base}/{residual}" if residual else base


def _repo_label(repo: Repository, scan_path: str, *, verbose: bool) -> str:
    """A column-1 repository label, carrying no internal ids in normal mode —
    see ``_repo_path_component``'s own docstring for the path half. A
    key-status hint (``· LABEL``, matching ``doctor``'s own subtitle
    style) is appended only once it's actually known. The layout-kind
    suffix is appended only in verbose mode — ``uuid``/``catalog_id``
    are shown per-catalog instead (see ``Catalog.info``), not at this
    whole-repository label."""
    label = _repo_path_component(repo, scan_path)
    key_hint = _KEY_STATUS_LABELS.get(repo.key_status)
    if key_hint is not None:
        label += f" · {key_hint}"
    if verbose:
        label += f" (layout: {repo.layout.kind.value})"
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
    vault or not."""
    if not verbose:
        return name
    return f"{name} (uuid: {catalog.info.uuid}, id: {catalog.connection.connection_id})"
