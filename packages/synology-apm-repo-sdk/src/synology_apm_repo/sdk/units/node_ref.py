"""``NodeRef`` — a stable, round-trippable string address for any browsable
node. CLI arguments, TUI breadcrumbs/bookmarks, and error messages all
share this one addressing scheme instead of a pile of mutually-exclusive
flags.

Three segment shapes share the same ``<repo-path>#<seg>/<seg>/...``
structure — which shape a ref is is entirely a property of its
segments' content, not a separate stored field:

======================================================  =====================================================
shape                                                   example
======================================================  =====================================================
canonical (``cat:``/``wl:``/``ver:`` prefixed)          ``repository#cat:1/wl:2/ver:abc-uid/Inbox/Subject``
raw (``file_map`` fallback axis, diagnostic-mode only)  ``repository#raw/VM-uid/.../disk.img``
human (display names, everything else)                  ``repository#Test-Workload-01/CORP-PC-001/2026-08-07 09:00``
======================================================  =====================================================

Canonical refs are stable across display-name changes — the right form
for scripts and bookmarks. Human refs are what breadcrumbs and ``ls``
show; ``disambiguate`` appends a hash suffix when two same-level nodes
would otherwise show the same name.

**Escaping**: a segment may itself contain ``#``, ``/``, ``%``, or control
characters — each is percent-encoded before segments join with ``/``,
decoded back on split, so round-tripping through ``str(ref)`` /
``NodeRef.parse`` is always safe. ``repo_path`` itself is never encoded
(split off at the *first* ``#``), so a literal ``#`` inside it can't
round-trip — accepted, since real filesystem paths essentially never
contain one. A lone empty-string segment (``NodeRef(path, ("",))``) also
isn't representable, since it encodes identically to no segments at all
— no real display name or path component is ever the empty string, so
this never arises.
"""

from __future__ import annotations

import dataclasses
import enum
import hashlib
from collections import Counter
from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, TypeVar
from urllib.parse import unquote

from ..identifiers import CatalogId, VersionUid, WorkloadId, resolve_catalog_id

if TYPE_CHECKING:
    from ..catalog.version import Version
    from ..catalog.workload import Workload
    from ..dedup.repository import DedupRepo

_RESERVED = frozenset("#/%")
_T = TypeVar("_T")


class RefKind(enum.Enum):
    """Which of the three segment shapes a ``NodeRef`` is: canonical
    (``cat:``/``wl:``/``ver:`` prefixed), raw (``file_map`` fallback axis,
    diagnostic-mode only), or human (display names, everything else)."""

    CANONICAL = "canonical"
    RAW = "raw"
    HUMAN = "human"


def _encode_segment(segment: str) -> str:
    out: list[str] = []
    for ch in segment:
        code = ord(ch)
        if ch in _RESERVED or code < 0x20 or code == 0x7F:
            out.extend(f"%{b:02X}" for b in ch.encode("utf-8"))
        else:
            out.append(ch)
    return "".join(out)


def _decode_segment(segment: str) -> str:
    return unquote(segment, errors="strict")


@dataclasses.dataclass(frozen=True)
class NodeRef:
    """``repo_path`` is a store-relative or filesystem path to the
    repository root (unencoded — it is a path, not ref content);
    ``segments`` are the decoded logical path components after the ``#``.
    """

    repo_path: str
    segments: tuple[str, ...] = ()

    def __str__(self) -> str:
        encoded = "/".join(_encode_segment(s) for s in self.segments)
        return f"{self.repo_path}#{encoded}"

    @classmethod
    def parse(cls, text: str) -> NodeRef:
        """Inverse of ``__str__``.

        Raises:
            ValueError: ``text`` has no ``#`` at all (not a ref).
        """
        if "#" not in text:
            raise ValueError(f"not a NodeRef (missing '#'): {text!r}")
        repo_path, fragment = text.split("#", 1)
        segments = tuple(_decode_segment(part) for part in fragment.split("/")) if fragment else ()
        return cls(repo_path, segments)

    @property
    def kind(self) -> RefKind:
        if not self.segments:
            return RefKind.HUMAN
        first = self.segments[0]
        if first == "raw":
            return RefKind.RAW
        if first.startswith("cat:"):
            return RefKind.CANONICAL
        return RefKind.HUMAN

    @property
    def canonical_ids(self) -> tuple[CatalogId, WorkloadId, VersionUid] | None:
        """``(catalog_id, workload_id, version_uid)`` if this is a
        canonical ref with a well-formed prefix, else ``None``.
        ``catalog_id`` is a plain string, never parsed as an int here — an
        object-storage ``CatalogId`` is a repo-id string, not always an
        integer the way a vault's ``connection_config_id`` is."""
        if self.kind is not RefKind.CANONICAL:
            return None
        try:
            catalog_id = self.segments[0].removeprefix("cat:")
            workload_id = int(self.segments[1].removeprefix("wl:"))
            version_uid = self.segments[2].removeprefix("ver:")
        except (IndexError, ValueError):
            return None
        return CatalogId(catalog_id), WorkloadId(workload_id), VersionUid(version_uid)

    @property
    def extra_segments(self) -> tuple[str, ...]:
        """Segments beyond the fixed prefix — the provider-internal path
        (mail folder, mail subject, ...) for canonical refs; the
        ``file_map`` path's own components (one per path segment — join
        with ``"/"`` to recover the literal path) for raw; everything, for
        human refs."""
        kind = self.kind
        if kind is RefKind.CANONICAL:
            return self.segments[3:]
        if kind is RefKind.RAW:
            return self.segments[1:]
        return self.segments

    # -- construction helpers ---------------------------------------

    @classmethod
    def canonical(
        cls,
        repo_path: str,
        *,
        catalog_id: CatalogId,
        workload_id: WorkloadId,
        version_uid: VersionUid,
        extra: Sequence[str] = (),
    ) -> NodeRef:
        return cls(
            repo_path,
            (f"cat:{catalog_id}", f"wl:{workload_id}", f"ver:{version_uid}", *extra),
        )

    @classmethod
    def raw(cls, repo_path: str, file_map_path: str) -> NodeRef:
        # split into its own path components rather than one opaque
        # segment, so the rendered ref reads as a normal-looking path
        # ("raw/VM-uid/dir/disk.img") instead of one percent-escaped blob.
        return cls(repo_path, ("raw", *file_map_path.split("/")))

    @classmethod
    def human(cls, repo_path: str, *names: str) -> NodeRef:
        return cls(repo_path, tuple(names))

    def child(self, *extra: str) -> NodeRef:
        """Extends this ref's segments by ``extra`` — for a provider
        appending segments to an already-resolved node's ref (a
        diagnostic leaf, synthetic entry, ...), not the version root
        (``canonical_ref_for`` covers that)."""
        return NodeRef(self.repo_path, (*self.segments, *extra))


def canonical_ref_for(repo: DedupRepo, version: Version, extra: Sequence[str] = ()) -> NodeRef:
    """``NodeRef.canonical`` for ``version``, within ``repo``.

    ``catalog_id`` (``identifiers.resolve_catalog_id``) is
    ``repo.layout.repo_id`` when set (object storage's own repo-id,
    unique per bucket), falling back to ``version.connection_config_id``
    otherwise — correct for a vault (whose ``connection_config_id`` is
    already unique within it) and for the one object-storage edge case
    with no derivable repo-id, since that case only arises when this
    ``DedupRepo`` is the sole catalog reachable from here."""
    catalog_id = resolve_catalog_id(repo.layout.repo_id, version.connection_config_id)
    return NodeRef.canonical(
        repo.layout.repo_root,
        catalog_id=catalog_id,
        workload_id=version.workload_id,
        version_uid=version.version_uid,
        extra=extra,
    )


class _HasCatalogIdentity(Protocol):
    """Structural stand-in for ``api.Catalog`` — this module can't import
    it directly (``api/`` sits *above* ``units/`` in this project's own
    layering; see ``ARCHITECTURE.md``), so ``catalog_pairs`` is typed
    against just the two attributes it actually reads instead."""

    @property
    def display_name(self) -> str: ...
    @property
    def catalog_id(self) -> CatalogId: ...


def catalog_pairs(catalogs: Sequence[_HasCatalogIdentity]) -> list[tuple[str, str]]:
    """``(display_name, catalog_id)`` pairs, in ``catalogs``' own order —
    feeds ``disambiguate()``/``match_display_name()``, shared by
    ``Repository.walk_human_ref`` and the CLI's ``ls``/``tree``."""
    return [(c.display_name, str(c.catalog_id)) for c in catalogs]


def workload_pairs(workloads: Sequence[Workload]) -> tuple[list[tuple[str, str]], list[str | None]]:
    """Same shape as ``catalog_pairs``, for ``Workload``, plus each
    workload's ``type_hint`` — feeds ``disambiguate()``'s ``hints``
    alongside its ``pairs``."""
    return [(w.display_name, w.workload_uid) for w in workloads], [w.type_hint for w in workloads]


def version_pairs(versions: Sequence[Version]) -> list[tuple[str, str]]:
    """Same shape as ``catalog_pairs``, for ``Version``."""
    return [(v.display_name, v.version_uid) for v in versions]


def disambiguate(names_and_ids: Sequence[tuple[str, str]], *, hints: Sequence[str | None] | None = None) -> list[str]:
    """Append a disambiguating suffix to every name that collides with
    another at the same tree level (the same call's list); names with no
    collision are returned unchanged. ``names_and_ids`` is
    ``(display_name, canonical_id)`` — the stable id, never the display
    name, is what the hash fallback is derived from, so a suffix stays
    the same across renames.

    ``hints`` (parallel, ``None`` where inapplicable) is a human
    differentiator such as ``sub_type``: one GWS/M365 account can produce
    several ``Workload`` rows sharing one ``display_name`` (the account's
    name/email), so a hash alone reads as duplicates. A hint that
    resolves the collision is used instead (``"Alice Example <...> · MAIL"``);
    the hash still applies, appended after the hint, to whatever the
    hint doesn't resolve.
    """
    resolved_hints: Sequence[str | None] = hints if hints is not None else [None] * len(names_and_ids)
    counts = Counter(name for name, _ in names_and_ids)

    candidates: list[str] = []
    for (name, _canonical_id), hint in zip(names_and_ids, resolved_hints, strict=True):
        if counts[name] > 1 and hint:
            candidates.append(f"{name} · {hint}")
        else:
            candidates.append(name)

    candidate_counts = Counter(candidates)

    result = []
    for (_name, canonical_id), candidate in zip(names_and_ids, candidates, strict=True):
        if candidate_counts[candidate] > 1:
            suffix = hashlib.sha256(canonical_id.encode("utf-8")).hexdigest()[:4]
            result.append(f"{candidate} #{suffix}")
        else:
            result.append(candidate)
    return result


def disambiguate_catalogs(catalogs: Sequence[_HasCatalogIdentity]) -> list[str]:
    """Disambiguated display names for ``catalogs``, in the same order.
    Zipping the result back against ``catalogs`` (or any other parallel
    sequence) is left to the caller."""
    return disambiguate(catalog_pairs(catalogs))


def disambiguate_workloads(workloads: Sequence[Workload], *, use_type_hint: bool = True) -> list[str]:
    """Same idea as ``disambiguate_catalogs``, for ``Workload``, folding
    in ``workload_pairs()``'s ``type_hint`` disambiguation hint by
    default. ``use_type_hint=False`` skips it — for a caller whose
    ``workloads`` are already grouped by ``type_hint`` (the browser's
    per-sub_type leaf list), where showing it again would just repeat
    what the grouping already conveys."""
    pairs, hints = workload_pairs(workloads)
    return disambiguate(pairs, hints=hints if use_type_hint else None)


def disambiguate_versions(versions: Sequence[Version]) -> list[str]:
    """Same idea as ``disambiguate_catalogs``, for ``Version``."""
    return disambiguate(version_pairs(versions))


def match_display_name(
    target: str,
    pairs: Sequence[tuple[str, str]],
    objects: Sequence[_T],
    *,
    hints: Sequence[str | None] | None = None,
) -> _T | None:
    """Match ``target`` against the *displayed* form of ``pairs``
    (``(display_name, stable_id)``) after running the collision suffix
    through ``disambiguate`` — the same transform the CLI/TUI apply, so a
    name copied straight out of a breadcrumb resolves back exactly.

    ``hints`` must match whatever the display side passed to its own
    ``disambiguate`` call for these same ``pairs``."""
    for name, obj in zip(disambiguate(pairs, hints=hints), objects, strict=True):
        if name == target:
            return obj
    return None


def ambiguous_matches(
    target: str,
    pairs: Sequence[tuple[str, str]],
    *,
    hints: Sequence[str | None] | None = None,
) -> list[str]:
    """The disambiguated names among ``pairs`` whose *pre-suffix* display
    name equals ``target``. Meant to be called only after
    ``match_display_name`` returned ``None`` for the same
    ``target``/``pairs``/``hints`` — under that precondition, a
    non-empty result means ``target`` collided at this level (real
    ambiguity); empty means it doesn't exist here at all."""
    return [name for (raw, _id), name in zip(pairs, disambiguate(pairs, hints=hints), strict=True) if raw == target]
