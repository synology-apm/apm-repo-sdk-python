"""Repository layout detection (FORMAT-SPEC.md: repo-root-layout).

Two shapes exist, both placed by fixed, product-enforced logic — never
nested arbitrarily deep:

- **Vault** (APV, or any local-filesystem Copy destination): a directory
  (conventionally ``@ActiveProtectVault``, though the name itself isn't
  load-bearing — detection is by content marker, not name) *is* the
  repository root directly, created exactly one level under the
  admin-chosen shared folder. Identified by the coexistence of
  ``repo_info``, ``link.key`` and ``.fully_created`` at that exact path.
  Exactly one repository per such root, and exactly one vault per shared
  folder.
- **Object storage** (S3/Azure landed copies): the root contains an
  ``@ActiveProtectData/<12-char-repo-id>/`` subtree, possibly with several
  sibling ``<repo-id>`` directories, and a parallel
  ``@ActiveProtectKey/{userKey,link}/`` tree one level up from
  ``@ActiveProtectData``. Each ``<repo-id>`` is an independent repository
  sharing the same key tree.

  ``repo_info`` under an object-store repository root is **not** a reliable
  marker by itself — it may carry a ``.<N>`` generation suffix and have no
  bare-named file at all. ``db/`` and ``@data`` are both always present as
  bare-named directories regardless of generation suffixing, so the pair
  of them is the marker used here (see
  ``_looks_like_object_store_repo``'s own comment for why both, not just
  one).

Since neither shape is ever nested more than one level under whatever an
admin actually provisioned (a shared folder, a bucket), a connection
pointed anywhere from *that* level down to two levels *above* it still
finds every repository. ``_DEFAULT_MAX_DEPTH`` bounds the walk at exactly
that — not the fully open-ended search a misdirected scan of an unrelated
directory tree could run away into, but not artificially limited to a
single level either.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import AsyncIterator

from ..errors import NotFoundError
from .base import ObjectStore, join_path

_DATA_DIR = "@ActiveProtectData"
_KEY_DIR = "@ActiveProtectKey"
_VAULT_MARKERS = ("repo_info", "link.key", ".fully_created")
_DEFAULT_MAX_DEPTH = 2


class RepoKind(enum.Enum):
    """Which of the two physical repository backends a ``RepoLayout``
    describes."""

    VAULT = "vault"
    OBJECT_STORE = "object_store"


@dataclasses.dataclass(frozen=True)
class RepoLayout:
    """Where one repository lives within an ``ObjectStore``.

    ``repo_root`` and ``key_root`` are store-relative paths (``""`` means
    "the store's own root"), never absolute paths — an ``ObjectStore``
    has no such notion.
    """

    kind: RepoKind
    repo_root: str
    key_root: str | None = None
    repo_id: str | None = None


async def _looks_like_vault_root(store: ObjectStore, path: str) -> bool:
    for marker in _VAULT_MARKERS:
        if not await store.exists(join_path(path, marker)):
            return False
    return True


async def _looks_like_object_store_repo(store: ObjectStore, repo_root: str) -> bool:
    # Both dirs are present regardless of repo_info suffixing; requiring
    # both (rather than just "db") reduces the chance of a false positive
    # on some unrelated directory that merely happens to contain a "db"
    # subdirectory of its own.
    return await store.exists(join_path(repo_root, "db")) and await store.exists(join_path(repo_root, "@data"))


async def _safe_listdir(store: ObjectStore, path: str) -> list[str]:
    try:
        return await store.listdir(path)
    except NotFoundError:
        return []


def _data_dir_ancestor(root: str) -> tuple[str, str] | None:
    """If ``root`` is itself an ``_DATA_DIR/<repo_id>`` path — a scan
    narrowed straight to one object-store repository directory, without its
    bucket-root ancestor — return ``(ancestor, repo_id)``, ``ancestor``
    being the directory ``_DATA_DIR`` and its sibling ``_KEY_DIR`` both
    live directly under.

    Returns ``None`` when ``root`` doesn't have that shape (fewer than
    two segments, or its parent segment isn't literally ``_DATA_DIR``) —
    the genuinely ambiguous case where no key tree can be located at all.
    """
    segments = root.split("/")
    if len(segments) < 2 or segments[-2] != _DATA_DIR:
        return None
    return "/".join(segments[:-2]), segments[-1]


async def _layouts_at(store: ObjectStore, root: str) -> list[RepoLayout] | None:
    """Classify ``root`` itself, non-recursively — the one repository-root-marker
    check ``iter_layouts``/``detect_layout`` both build on.

    Returns ``None`` when ``root`` matches no recognized repository-root shape
    at all — the caller decides what that means (``iter_layouts`` recurses
    into children, ``detect_layout`` raises). Otherwise a list of the
    layout(s) found directly at ``root``: always exactly one for a vault
    root or for ``root`` itself being an individual object-store repository
    directory, but possibly zero or several for an ``@ActiveProtectData``
    parent — its own repo-id subdirectories may or may not each pass
    ``_looks_like_object_store_repo``, and either way this is the end of
    the walk along this branch (no repository nests inside another), never a
    reason to recurse further.
    """
    if await _looks_like_vault_root(store, root):
        return [RepoLayout(kind=RepoKind.VAULT, repo_root=root)]

    data_dir = join_path(root, _DATA_DIR)
    if await store.exists(data_dir):
        key_dir = join_path(root, _KEY_DIR)
        resolved_key_dir = key_dir if await store.exists(key_dir) else None
        layouts = []
        for repo_id in sorted(await _safe_listdir(store, data_dir)):
            repo_root = join_path(data_dir, repo_id)
            if await _looks_like_object_store_repo(store, repo_root):
                layouts.append(
                    RepoLayout(
                        kind=RepoKind.OBJECT_STORE, repo_root=repo_root, key_root=resolved_key_dir, repo_id=repo_id
                    )
                )
        return layouts

    if await _looks_like_object_store_repo(store, root):
        # ``root`` is itself an individual object-store repository directory. Its
        # sibling @ActiveProtectKey/ lives one level *above* @ActiveProtectData
        # (never under the repo id itself), so when `root` is recognizably an
        # `@ActiveProtectData/<repoId>` path, look for it there instead of
        # under `root` — the same store still reaches it, `root` only narrows
        # where the scan started. Otherwise (root's parent segment isn't
        # literally @ActiveProtectData — a caller pointed straight at some
        # other, unrelated repository directory) the key tree truly is unreachable
        # from this store; supply key material some other way, or reopen a
        # store rooted at the bucket level to get key_root/repo_id populated.
        ancestor = _data_dir_ancestor(root)
        key_root = ancestor[0] if ancestor is not None else root
        narrowed_repo_id = ancestor[1] if ancestor is not None else None
        key_dir = join_path(key_root, _KEY_DIR)
        return [
            RepoLayout(
                kind=RepoKind.OBJECT_STORE,
                repo_root=root,
                key_root=key_dir if await store.exists(key_dir) else None,
                repo_id=narrowed_repo_id,
            )
        ]

    return None


async def iter_layouts(
    store: ObjectStore,
    root: str = "",
    *,
    max_depth: int = _DEFAULT_MAX_DEPTH,
) -> AsyncIterator[RepoLayout]:
    """Walk down from ``root`` looking for repository roots, yielding each
    as it is found.

    Cheap by construction: only ``exists()``/``listdir()`` calls at each
    level, never a scan into ``Pool``/``Composition``/``db``. A vault root
    or object-store bucket root ends the walk along that branch (no repository
    nests inside another); ``max_depth`` bounds how far it goes otherwise
    (see this module's own docstring for why exactly this far).

    Total result count is unknowable in advance — callers doing interactive
    discovery should treat this as an indeterminate-progress operation and
    consume it lazily.
    """
    layouts = await _layouts_at(store, root)
    if layouts is not None:
        for layout in layouts:
            yield layout
        return

    if max_depth <= 0:
        return
    for child in sorted(await _safe_listdir(store, root)):
        # ``yield from`` is not available in an async generator — the async
        # equivalent of delegating to a recursive sub-generator is this
        # explicit ``async for`` re-yield loop.
        async for layout in iter_layouts(store, join_path(root, child), max_depth=max_depth - 1):
            yield layout


async def detect_layout(store: ObjectStore, root: str = "") -> RepoLayout:
    """Detect the single repository layout directly at ``root`` — for the
    common case where the caller already knows ``root`` is exactly one
    repository (e.g. it was chosen from a prior ``iter_layouts`` scan).

    Raises ``NotFoundError`` if ``root`` is not itself a repository root, or is an
    object-store bucket root containing more than one (or zero) repository ids —
    callers facing that ambiguity should use ``iter_layouts`` instead.
    """
    layouts = await _layouts_at(store, root)
    if layouts is None:
        raise NotFoundError("no repository layout detected", ref=root)
    if len(layouts) != 1:
        raise NotFoundError(
            f"contains {len(layouts)} valid repository id(s) under {_DATA_DIR} — ambiguous; "
            "use iter_layouts() and pick one",
            ref=root,
        )
    return layouts[0]


# -- Repository/Catalog model (see the project's own plan file) ------------
#
# `RepositoryLayout` below is the Repository-level counterpart to
# `RepoLayout` above. The internal switch-over is done (`api/session.py`/
# `api/repository.py` build exclusively on the functions below now) --
# `RepoLayout`/`iter_layouts`/`detect_layout` stay only as public SDK
# exports (`sdk/__init__.py`/`sdk/storage/__init__.py`), kept for an
# external caller that still wants the narrower, one-catalog-at-a-time
# shape rather than removed outright. Where `RepoLayout` represents one
# *catalog*-level location (one vault, or one individual `<repo-id>`
# directory -- the level a caller has to disambiguate down to one of, by
# hand, when a bucket holds several), `RepositoryLayout` represents the
# *Repository* level: one vault (still 1:1, no change from today), or one
# whole object-store bucket, carrying every sibling `<repo-id>` it found
# as `catalog_ids` rather than requiring the caller to have already picked
# one.


@dataclasses.dataclass(frozen=True)
class RepositoryLayout:
    """Where one Repository (a bucket, or a vault's own shared folder —
    see this module's own docstring) lives within an ``ObjectStore``.

    ``repo_root``/``key_root`` are store-relative paths (``""`` means "the
    store's own root"), never absolute — same convention as
    ``RepoLayout``.

    ``catalog_ids`` is populated for ``OBJECT_STORE`` by listing
    ``@ActiveProtectData/``'s children (cheap — already known at
    detection time, the same listing ``_layouts_at``'s own second branch
    already does); left ``None`` for ``VAULT``, whose catalogs
    (``db/connection_config`` rows) can only be found later, by querying
    after opening — and also left ``None`` for the one genuinely
    ambiguous ``OBJECT_STORE`` case: ``root`` is itself an individual
    repository directory with no derivable bucket-root ancestor (see
    ``_data_dir_ancestor``), where the one implicit catalog living there
    can't be independently identified either. An empty list (as opposed
    to ``None``) means a real, listable ``@ActiveProtectData`` was found
    but currently has zero valid repo-id children.
    """

    kind: RepoKind
    repo_root: str
    key_root: str | None = None
    catalog_ids: list[str] | None = None


async def _repository_layout_at(store: ObjectStore, root: str) -> RepositoryLayout | None:
    """Classify ``root`` itself, non-recursively — the Repository-level
    counterpart to ``_layouts_at``. Returns at most one ``RepositoryLayout``
    per ``root`` (never several: a location is a vault, a bucket, or
    nothing — never multiple distinct Repositories sharing one root).

    Returns ``None`` when ``root`` matches no recognized repository-root shape
    at all — same contract as ``_layouts_at``.
    """
    if await _looks_like_vault_root(store, root):
        return RepositoryLayout(kind=RepoKind.VAULT, repo_root=root)

    data_dir = join_path(root, _DATA_DIR)
    if await store.exists(data_dir):
        key_dir = join_path(root, _KEY_DIR)
        resolved_key_dir = key_dir if await store.exists(key_dir) else None
        catalog_ids = [
            repo_id
            for repo_id in sorted(await _safe_listdir(store, data_dir))
            if await _looks_like_object_store_repo(store, join_path(data_dir, repo_id))
        ]
        return RepositoryLayout(
            kind=RepoKind.OBJECT_STORE, repo_root=root, key_root=resolved_key_dir, catalog_ids=catalog_ids
        )

    if await _looks_like_object_store_repo(store, root):
        # `root` is itself one object-store catalog directory. If it's
        # recognizably an `@ActiveProtectData/<repoId>` path, the real
        # Repository is the bucket root two levels up -- redirect there
        # and list every sibling catalog found under it (the same as the
        # branch above), rather than reporting only the one catalog this
        # particular narrowed `root` happened to hit: the Repository is
        # the same whole bucket regardless of which catalog a caller's
        # own scan started at.
        ancestor = _data_dir_ancestor(root)
        if ancestor is not None:
            bucket_root, _narrowed_repo_id = ancestor
            return await _repository_layout_at(store, bucket_root)
        # Genuinely ambiguous: no derivable bucket-root ancestor, so
        # neither the key tree nor this catalog's own canonical id can be
        # located from here — report it as its own, single, unenumerable
        # implicit catalog (`catalog_ids=None`).
        key_dir = join_path(root, _KEY_DIR)
        return RepositoryLayout(
            kind=RepoKind.OBJECT_STORE, repo_root=root, key_root=key_dir if await store.exists(key_dir) else None
        )

    return None


async def iter_repository_layouts(
    store: ObjectStore,
    root: str = "",
    *,
    max_depth: int = _DEFAULT_MAX_DEPTH,
) -> AsyncIterator[RepositoryLayout]:
    """Walk down from ``root`` looking for Repository roots (a bucket, or
    a vault's own shared folder), yielding each as it is found — the
    Repository-level counterpart to ``iter_layouts``.

    Unlike ``iter_layouts``, a bucket holding several sibling catalogs
    yields *one* ``RepositoryLayout`` (with ``catalog_ids`` listing every
    sibling found), never several — there is no "which repository id" ambiguity
    at this level to resolve.
    """
    layout = await _repository_layout_at(store, root)
    if layout is not None:
        yield layout
        return

    if max_depth <= 0:
        return
    for child in sorted(await _safe_listdir(store, root)):
        async for layout in iter_repository_layouts(store, join_path(root, child), max_depth=max_depth - 1):
            yield layout


def catalog_repo_layouts(layout: RepositoryLayout) -> list[RepoLayout]:
    """The individual, directly-openable ``RepoLayout``\\(s) ``layout``
    resolves to — exactly what ``DedupRepo.open()`` already consumes
    unchanged; this is the *only* place ``RepositoryLayout`` and
    ``RepoLayout`` meet.

    A vault (or the one genuinely ambiguous ``OBJECT_STORE`` case —
    ``catalog_ids`` is ``None``) opens as a single ``RepoLayout`` at
    ``layout.repo_root`` itself: a vault's own catalogs come from
    querying ``db/connection_config`` after opening, not a separate
    directory per catalog. Object storage opens one ``RepoLayout`` per
    ``catalog_ids`` entry, each rooted at that catalog's own
    ``@ActiveProtectData/<repo-id>`` subdirectory — every sibling shares
    ``layout.key_root``, since the key tree lives one level above
    ``@ActiveProtectData``, not inside any one repo-id's own directory.

    Returns an empty list only when ``layout.catalog_ids`` is itself a
    real, empty list (a listable ``@ActiveProtectData`` with zero valid
    repo-id children) — nothing to open at all in that case.
    """
    if layout.kind is RepoKind.VAULT or layout.catalog_ids is None:
        return [RepoLayout(kind=layout.kind, repo_root=layout.repo_root, key_root=layout.key_root)]
    data_dir = join_path(layout.repo_root, _DATA_DIR)
    return [
        RepoLayout(
            kind=RepoKind.OBJECT_STORE,
            repo_root=join_path(data_dir, catalog_id),
            key_root=layout.key_root,
            repo_id=catalog_id,
        )
        for catalog_id in layout.catalog_ids
    ]


def key_probe_layout(layout: RepositoryLayout) -> RepoLayout:
    """A throwaway ``RepoLayout`` carrying only the fields ``dedup.keys``'
    free functions (``probe_encrypted``/``resolve_vault_key``/``verify``)
    actually read for key/encryption resolution, straight off a
    bucket-rooted ``RepositoryLayout`` — safe to build with no catalog
    opened at all: ``VAULT`` reads ``repo_root`` (a vault's own
    ``repo_root`` is the same value either way), ``OBJECT_STORE`` reads
    only ``key_root`` (shared by every sibling catalog, never
    ``repo_root``). Used both before a ``Repository`` exists yet
    (``api.session``'s own discovery-time key/encryption probe) and by an
    already-constructed one (``Repository.set_key()``) — the one shared
    place this derivation lives, rather than two copies drifting apart.
    """
    return RepoLayout(kind=layout.kind, repo_root=layout.repo_root, key_root=layout.key_root)


async def detect_repository_layout(store: ObjectStore, root: str = "") -> RepositoryLayout:
    """Detect the single Repository layout directly at ``root`` — the
    Repository-level counterpart to ``detect_layout``.

    Raises ``NotFoundError`` if ``root`` is not itself a repository root.
    Unlike ``detect_layout``, never raises for "several repository ids found"
    — that's no longer ambiguous at the Repository level, it's
    ``catalog_ids`` having length greater than one; a caller wanting a
    single, already-disambiguated catalog picks one from ``catalog_ids``
    (or queries them after opening, for a vault) rather than re-scanning
    at a narrower root.
    """
    layout = await _repository_layout_at(store, root)
    if layout is None:
        raise NotFoundError("no repository layout detected", ref=root)
    return layout
