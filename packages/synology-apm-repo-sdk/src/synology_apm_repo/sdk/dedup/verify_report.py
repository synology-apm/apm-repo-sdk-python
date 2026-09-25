"""Grouping and a stable render order for a batch of ``Finding``\\ s from one
``Repository.verify()``/``Catalog.verify()`` run — the presentation-adjacent
logic that turns a flat list into "same root cause" groups, shared by every
frontend that lists ``Finding``\\ s this way (the CLI's own ``verify``
command and the TUI's own diagnostics screen both use it).

Kept here, next to ``verify_checks.py``'s own ``Finding``/``Stage``/
``Symptom`` definitions, rather than in ``presentation/`` — that package is
a true leaf with zero internal imports of its own (see ``ARCHITECTURE.md``'s
Presentation section), and this module needs ``Finding``, which lives one
layer up, in ``dedup``.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Sequence

from ..presentation.format import pluralize
from .verify_checks import Finding, Stage, Symptom

_STAGE_ORDER = {stage: i for i, stage in enumerate(Stage)}
_SYMPTOM_ORDER = {symptom.value: i for i, symptom in enumerate(Symptom)}

#: An instance-specific value a ``Finding.detail`` sentence embeds: a
#: quoted path/name, a bracketed ``[ref=...]``/``[spec=...]`` tag (see
#: ``ApmRepoError.__str__``), or a bare run of digits (a snapshot_id/
#: version_id pair, an offset, ...). Quoted/bracketed spans are tried
#: first in the alternation so a digit run *inside* one of them is
#: consumed as part of it, not matched again on its own. The quoted-span
#: alternative is backslash-escape-aware (``(?:[^'\\]|\\.)*``, not a bare
#: ``[^']*``): some raise sites format a path via ``{path!r}``, and
#: Python's own ``repr()`` keeps ``'`` as the delimiter and backslash-escapes
#: an internal ``'`` whenever the string also contains a literal ``"`` (e.g.
#: a real filename like ``O'Brien's "backup".pst``) -- a non-escape-aware
#: pattern would stop at that escaped quote instead of the real closing one,
#: splitting one quoted value into two bogus matches.
_VARIABLE_RE = re.compile(r"'(?:[^'\\]|\\.)*'|\[(?:ref|spec)=[^\]]*\]|\d+")

#: Just the value half of an embedded ``[ref=...]`` tag -- when a finding's
#: detail carries one, it names the exact missing/stale object the SDK
#: itself resolved, a strictly more precise "same root cause" signal than
#: any regex-inferred textual resemblance (see ``_group_key``).
_REF_TAG_RE = re.compile(r"\[ref=([^\]]*)\]")

#: A per-version Stage.VERSION finding's path is "<workload display_name>/
#: <version display_name>", and Version.display_name is a fixed
#: "YYYY-MM-DD HH:MM:SS" -- but not every Stage.VERSION finding is
#: per-version: a connection/workload-enumeration failure or a PC/PS
#: per-disk failure (units/verify_reachable.py's own _unresolvable_finding()
#: call sites) uses repo_root/connection.display_name/workload.display_name/
#: a "PC/PS disk '...'" label instead, none of which end in a timestamp --
#: matching this shape, not just trusting the last "/"-separated segment,
#: is what tells the two apart.
_TIMESTAMP_SUFFIX_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


def _normalize_detail(detail: str, *, keep_ref: bool = False) -> str:
    """``detail`` with every instance-specific value (see ``_VARIABLE_RE``)
    replaced by a placeholder — the "same reason, different instance"
    signature ``group_findings`` falls back to grouping findings by when no
    ``ref`` is available (see ``_group_key``). ``keep_ref=True`` leaves an
    embedded ``[ref=...]`` tag exactly as written instead of placeholdering
    it away — used to show a ref-based group's own concrete, already-known
    ref value in its header rather than hiding it behind ``[ref]``."""

    def repl(match: re.Match[str]) -> str:
        text = match.group(0)
        if text.startswith("'"):
            return "'…'"
        if text.startswith("["):
            tag = text[1:].split("=", 1)[0]
            if keep_ref and tag == "ref":
                return text
            return f"[{tag}]"
        return "#"

    return _VARIABLE_RE.sub(repl, detail)


def _variable_parts(detail: str, *, ref_value: str | None = None) -> str:
    """The instance-specific values ``_normalize_detail`` replaced, in
    order, joined for display under a group's shared header — the part of
    ``detail`` that still differs per instance. ``ref_value``, when given,
    is the group's own already-shown ref (its header displays it once) —
    both the ``[ref=...]`` tag itself and a quoted value equal to it (the
    common case: a raise site quotes the very path it also passes as
    ``ref``) are omitted here, since showing either again per instance
    would just repeat what the header already said."""
    parts = [
        match.group(0)
        for match in _VARIABLE_RE.finditer(detail)
        if ref_value is None or match.group(0) not in (f"[ref={ref_value}]", f"'{ref_value}'")
    ]
    return " ".join(parts)


def sort_key(finding: Finding) -> tuple[int, int, str, str, str]:
    """A stable, deterministic order for a batch of ``Finding``\\ s:
    chronological (by the version-display-name timestamp embedded in
    ``path``) for a genuine per-version finding, alphabetical by whole
    ``path`` for everything else (RepoInfo/FileMap/Composition/EncryptKey,
    a Bucket finding not tied to one version, and a ``Stage.VERSION``
    finding that isn't per-version either — a connection/workload
    enumeration failure or a PC/PS per-disk failure, neither of which ends
    in a timestamp). Also what makes ``--level full``'s own multiprocess
    bucket sweep produce a stable order despite not otherwise guaranteeing
    one."""
    candidate = finding.path.rsplit("/", 1)[-1]
    timeish = candidate if finding.stage is Stage.VERSION and _TIMESTAMP_SUFFIX_RE.search(candidate) else finding.path
    return (_STAGE_ORDER[finding.stage], _SYMPTOM_ORDER[finding.symptom.value], timeish, finding.path, finding.detail)


@dataclasses.dataclass(frozen=True)
class AugmentedFinding:
    """One ``Finding`` plus the ref/template/variable-parts signal
    ``group_findings``'s own grouping and every caller's rendering both
    need, computed once here rather than independently re-derived once per
    consumer (once during grouping, again per group representative at
    render time).

    Attributes:
        finding: The underlying ``Finding``.
        ref_value: This finding's own embedded ``[ref=...]`` value, or
            ``None`` for the minority of raise sites that never attach one
            (``units/fs.py``, ``units/saas/objectdb.py``).
        template: ``finding.detail`` with its instance-specific values
            placeholdered away, keeping an embedded ``[ref=...]`` tag
            visible exactly when ``ref_value`` is not ``None`` — this one
            ``keep_ref`` choice is what lets a single field serve both
            ``group_findings``'s own ``ref_value is None`` fallback
            discriminator and a group header's own display, which wants
            the ref tag kept visible when there is one.
        variable_parts: The instance-specific values ``template``
            replaced, in order, joined for display under a group's shared
            header — the part of ``finding.detail`` that still differs
            per instance, with ``ref_value`` itself omitted (the header
            already shows it once).
    """

    finding: Finding
    ref_value: str | None
    template: str
    variable_parts: str


def _augment(finding: Finding) -> AugmentedFinding:
    ref_match = _REF_TAG_RE.search(finding.detail)
    ref_value = ref_match.group(1) if ref_match else None
    template = _normalize_detail(finding.detail, keep_ref=ref_value is not None)
    variable_parts = _variable_parts(finding.detail, ref_value=ref_value)
    return AugmentedFinding(finding=finding, ref_value=ref_value, template=template, variable_parts=variable_parts)


def _group_key(aug: AugmentedFinding) -> tuple[Stage, str, str]:
    """``(stage, symptom, discriminator)`` — ``discriminator`` is the
    finding's own embedded ``[ref=...]`` value when its detail carries one
    (an exact "same missing/stale object" match, not a resemblance), or a
    normalized-template fallback otherwise, for the minority of raise sites
    that never attach a ref (``units/fs.py``, ``units/saas/objectdb.py``).
    The ``"ref:"``/``"tpl:"`` prefix is cheap, permanent insurance against
    a template string ever colliding with a real ref-based key — not a
    real expected collision today."""
    discriminator = f"ref:{aug.ref_value}" if aug.ref_value is not None else f"tpl:{aug.template}"
    return (aug.finding.stage, aug.finding.symptom.value, discriminator)


def group_findings(findings: Sequence[Finding]) -> list[list[AugmentedFinding]]:
    """Buckets ``findings`` (expected already sorted by ``sort_key``) by
    ``_group_key`` — findings that resolve to the same key (the same
    ``ref``, or the same normalized-template fallback) land in the same
    group. Groups themselves are then sorted by that same key (stage, then
    symptom, then the ref/template discriminator itself) rather than left
    in first-member-encountered order: two groups sharing a stage and
    symptom otherwise interleave by whichever member happened to sort
    chronologically first, which — for a SaaS stream's own sequential
    ``ref`` values — can put e.g. ``.../1/16/saas_obj`` ahead of
    ``.../1/14/saas_obj`` merely because one workload's display name
    happens to sort before the other's. Sorting groups by their own
    ref/template value directly avoids that, at the cost of no longer
    being purely chronological across groups (each group's own *members*
    stay chronological either way, unaffected by this).

    Returns:
        Each finding wrapped in its own ``AugmentedFinding``, so the
        ref/template/variable-parts signal is computed once here instead
        of being re-derived independently by each caller that renders
        these groups.
    """
    groups: dict[tuple[Stage, str, str], list[AugmentedFinding]] = {}
    for finding in findings:
        aug = _augment(finding)
        groups.setdefault(_group_key(aug), []).append(aug)

    # Sorts the already-computed keys themselves -- each finding's
    # _group_key() ran exactly once, above; no need to recompute it per
    # group here too.
    def key_sort_key(key: tuple[Stage, str, str]) -> tuple[int, int, str]:
        stage, symptom, discriminator = key
        return (_STAGE_ORDER[stage], _SYMPTOM_ORDER[symptom], discriminator)

    return [groups[key] for key in sorted(groups, key=key_sort_key)]


def group_count_label(group: Sequence[AugmentedFinding]) -> str:
    """``"<N> version(s)"`` for a per-version ``Stage.VERSION`` group, else
    ``"<N> occurrence(s)"`` — the pluralized member-count label every
    ``group_findings`` renderer shows under a group's own header."""
    noun = "version" if group[0].finding.stage is Stage.VERSION else "occurrence"
    return f"{len(group)} {pluralize(len(group), noun)}"
