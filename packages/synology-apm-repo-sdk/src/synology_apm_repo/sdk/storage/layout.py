"""Repository layout detection (FORMAT-SPEC.md: Locating the repository root).

Two shapes exist, both at a fixed depth:

- **Vault** (APV, or any local-filesystem Copy destination): a directory
  (conventionally ``@ActiveProtectVault``; detection is by marker, not name)
  *is* the repository root, one level under the admin-chosen shared folder.
  Identified by ``repo_info``, ``link.key`` and ``.fully_created`` together
  at that path. One repository per root, one vault per shared folder.
- **Object storage** (S3/Azure landed copies): the root contains an
  ``@ActiveProtectData/<12-char-repo-id>/`` subtree, possibly with several
  sibling ``<repo-id>`` directories, and a parallel
  ``@ActiveProtectKey/{userKey,link}/`` tree one level up from
  ``@ActiveProtectData``. Each ``<repo-id>`` is an independent repository
  sharing the same key tree.

  ``repo_info`` is not a reliable marker there (it may exist only as
  ``repo_info.<N>``). ``db/`` and ``@data`` are always bare-named
  directories, so the pair is the marker; requiring both guards against
  unrelated directories that merely contain ``db``.

Neither shape nests more than one level under what an admin provisioned (a
shared folder, a bucket), so a connection pointed up to two levels above
that still finds every repository. ``_DEFAULT_MAX_DEPTH`` bounds the walk at
that, so a misdirected scan of an unrelated tree can't run away.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import AsyncGenerator

from ..errors import NotFoundError
from .base import ObjectStore, join_path, list_names

_DATA_DIR = "@ActiveProtectData"
_VAULT_DIR = "@ActiveProtectVault"
_KEY_DIR = "@ActiveProtectKey"
REPO_INFO_NAME = "repo_info"
"""The logical name of the repository-root ``repo_info`` file, as
FORMAT-SPEC.md's repo_info section defines it."""
_VAULT_MARKERS = (REPO_INFO_NAME, "link.key", ".fully_created")
_DEFAULT_MAX_DEPTH = 2


class RepoKind(enum.Enum):
    """Which of the two physical repository backends a ``RepoLayout``
    describes."""

    VAULT = "vault"
    OBJECT_STORE = "object_store"


@dataclasses.dataclass(frozen=True, slots=True)
class RepoLayout:
    """Where one catalog (a vault, or one ``<repo-id>`` directory) lives
    within an ``ObjectStore``: what ``DedupRepo.open()`` takes. Built from a
    ``RepositoryLayout`` by ``catalog_repo_layouts``.

    Attributes:
        kind: Vault or object storage.
        repo_root: Store-relative repository root (``""``: the store root).
        key_root: Store-relative ``@ActiveProtectKey`` directory for object
            storage, or ``None`` if not found or not applicable.
        repo_id: The 12-character repository id for object storage, when known.
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
    return await store.exists(join_path(repo_root, "db")) and await store.exists(join_path(repo_root, "@data"))


async def _safe_listdir(store: ObjectStore, path: str) -> list[str]:
    try:
        return await list_names(store, path)
    except NotFoundError:
        return []


def _data_dir_ancestor(root: str) -> tuple[str, str] | None:
    """If ``root`` is a ``_DATA_DIR/<repo_id>`` path (a scan narrowed to one
    repository directory), return ``(ancestor, repo_id)``, where ``ancestor``
    holds both ``_DATA_DIR`` and ``_KEY_DIR``. Returns ``None`` otherwise,
    when no key tree can be located.
    """
    segments = root.split("/")
    if len(segments) < 2 or segments[-2] != _DATA_DIR:
        return None
    return "/".join(segments[:-2]), segments[-1]


async def _bucket_at(store: ObjectStore, root: str) -> tuple[str | None, list[str]] | None:
    """For a bucket root (one holding ``@ActiveProtectData``), its
    ``@ActiveProtectKey`` directory (``None`` if absent) and its valid
    repository ids, sorted; ``None`` when ``root`` is not a bucket root."""
    data_dir = join_path(root, _DATA_DIR)
    if not await store.exists(data_dir):
        return None
    key_dir = join_path(root, _KEY_DIR)
    resolved_key_dir = key_dir if await store.exists(key_dir) else None
    repo_ids = [
        repo_id
        for repo_id in sorted(await _safe_listdir(store, data_dir))
        if await _looks_like_object_store_repo(store, join_path(data_dir, repo_id))
    ]
    return resolved_key_dir, repo_ids


@dataclasses.dataclass(frozen=True, slots=True)
class RepositoryLayout:
    """Where one Repository (a bucket, or a vault's own shared folder)
    lives within an ``ObjectStore``.

    Attributes:
        kind: Vault or object storage.
        repo_root: Store-relative root (``""``: the store root).
        key_root: Store-relative ``@ActiveProtectKey`` directory, or ``None``.
        catalog_ids: For ``OBJECT_STORE``, the valid repo-id children of
            ``@ActiveProtectData/`` (an empty list: none). ``None`` for
            ``VAULT`` (catalogs come from ``db/connection_config``) and when
            ``root`` is an individual repository directory with no derivable
            bucket-root ancestor (see ``_data_dir_ancestor``).
    """

    kind: RepoKind
    repo_root: str
    key_root: str | None = None
    catalog_ids: list[str] | None = None

    @property
    def display_root(self) -> str:
        """``repo_root`` without its ``@ActiveProtectVault``/
        ``@ActiveProtectData`` segments, at any depth: the part worth showing
        a user (the folders above the repository, an object-storage repo
        id); ``""`` for a vault at the store root."""
        return "/".join(s for s in self.repo_root.split("/") if s not in (_VAULT_DIR, _DATA_DIR))


async def _repository_layout_at(store: ObjectStore, root: str) -> RepositoryLayout | None:
    """Classify ``root`` itself, non-recursively. A root is a vault, a
    bucket, or nothing, so there is at most one result; ``None`` means no
    recognized shape.
    """
    if await _looks_like_vault_root(store, root):
        return RepositoryLayout(kind=RepoKind.VAULT, repo_root=root)

    if (bucket := await _bucket_at(store, root)) is not None:
        key_root, repo_ids = bucket
        return RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root=root, key_root=key_root, catalog_ids=repo_ids)

    if await _looks_like_object_store_repo(store, root):
        # ``root`` is one catalog directory; if it is @ActiveProtectData/<repoId>,
        # the Repository is the whole bucket two levels up.
        ancestor = _data_dir_ancestor(root)
        if ancestor is not None:
            bucket_root, _narrowed_repo_id = ancestor
            return await _repository_layout_at(store, bucket_root)
        # No bucket-root ancestor: a single catalog (`catalog_ids=None`).
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
) -> AsyncGenerator[RepositoryLayout, None]:
    """Walk down from ``root`` looking for Repository roots (a bucket, or a
    vault's shared folder), yielding each as found. A bucket with several
    catalogs yields one ``RepositoryLayout`` whose ``catalog_ids`` lists them.

    Only ``exists()``/``listdir()`` calls, never a scan into ``Pool``/
    ``Composition``/``db``. A found root ends the walk along its branch;
    otherwise ``max_depth`` (default 2, see the module docstring) bounds it.
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
    """The ``RepoLayout``\\(s) ``layout`` resolves to, each openable by
    ``DedupRepo.open()``.

    A vault, or an ``OBJECT_STORE`` layout with ``catalog_ids`` of ``None``,
    yields one ``RepoLayout`` at ``layout.repo_root`` (a vault's catalogs
    come from ``db/connection_config`` after opening). Otherwise one per
    ``catalog_ids`` entry, rooted at its ``@ActiveProtectData/<repo-id>``
    directory and sharing ``layout.key_root``. Empty only when
    ``catalog_ids`` is an empty list.
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
    """A ``RepoLayout`` carrying only the fields ``dedup.keys`` reads for key
    and encryption resolution (``probe_encrypted``/``resolve_vault_key``/
    ``verify``), built from a ``RepositoryLayout`` without opening a catalog.
    Used by discovery's key probe and ``Repository.set_key()``.
    """
    return RepoLayout(kind=layout.kind, repo_root=layout.repo_root, key_root=layout.key_root)


async def detect_repository_layout(store: ObjectStore, root: str = "") -> RepositoryLayout:
    """Detect the Repository layout at ``root``. A bucket's several
    repository ids are not an error; they appear in ``catalog_ids``.

    Raises:
        NotFoundError: ``root`` is not a repository root.
    """
    layout = await _repository_layout_at(store, root)
    if layout is None:
        raise NotFoundError("no repository layout detected", ref=root)
    return layout
