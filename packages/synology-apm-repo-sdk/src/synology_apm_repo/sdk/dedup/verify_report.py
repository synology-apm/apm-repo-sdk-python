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
#: quoted path/name, a bracketed ``[ref=...]``/``[spec=...]`` tag, or a
#: bare run of digits. Quoted/bracketed spans are tried first so a digit
#: run inside one isn't matched again on its own. The quoted-span
#: alternative is backslash-escape-aware (``(?:[^'\\]|\\.)*``): some raise
#: sites format a path via ``{path!r}``, and Python's ``repr()`` escapes
#: an internal ``'`` when the string also contains a literal ``"`` (e.g.
#: ``O'Brien's "backup".pst``) — a non-escape-aware pattern would stop at
#: that escaped quote instead of the real closing one.
_VARIABLE_RE = re.compile(r"'(?:[^'\\]|\\.)*'|\[(?:ref|spec)=[^\]]*\]|\d+")

#: Just the value half of an embedded ``[ref=...]`` tag -- when a finding's
#: detail carries one, it names the exact missing/stale object the SDK
#: itself resolved, a strictly more precise "same root cause" signal than
#: any regex-inferred textual resemblance (see ``_group_key``).
_REF_TAG_RE = re.compile(r"\[ref=([^\]]*)\]")

#: A per-version ``Stage.VERSION`` finding's path ends in
#: ``Version.display_name``'s fixed ``"YYYY-MM-DD HH:MM:SS"`` format, but
#: not every ``Stage.VERSION`` finding is per-version — a connection/
#: workload-enumeration or PC/PS per-disk failure uses a different label
#: shape that never ends in a timestamp. Matching this shape, not just
#: trusting the last "/"-separated segment, is what tells the two apart.
_TIMESTAMP_SUFFIX_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


def _normalize_detail(detail: str, *, keep_ref: bool = False) -> str:
    """``detail`` with every instance-specific value (see ``_VARIABLE_RE``)
    replaced by a placeholder — the "same reason, different instance"
    signature ``group_findings`` falls back to when no ``ref`` is
    available (see ``_group_key``). ``keep_ref=True`` leaves an embedded
    ``[ref=...]`` tag as written, to show a ref-based group's own
    concrete value in its header rather than hiding it."""

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
    order, joined for display under a group's shared header.
    ``ref_value``, when given, omits both the ``[ref=...]`` tag and a
    quoted value equal to it, since the header already shows it once."""
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
    ``path`` otherwise. Also what makes ``--level full``'s multiprocess
    bucket sweep produce a stable order despite not otherwise guaranteeing
    one."""
    candidate = finding.path.rsplit("/", 1)[-1]
    timeish = candidate if finding.stage is Stage.VERSION and _TIMESTAMP_SUFFIX_RE.search(candidate) else finding.path
    return (_STAGE_ORDER[finding.stage], _SYMPTOM_ORDER[finding.symptom.value], timeish, finding.path, finding.detail)


@dataclasses.dataclass(frozen=True)
class AugmentedFinding:
    """One ``Finding`` plus the ref/template/variable-parts signal
    ``group_findings``'s grouping and every caller's rendering both need,
    computed once here instead of independently re-derived per consumer.

    Attributes:
        finding: The underlying ``Finding``.
        ref_value: This finding's own embedded ``[ref=...]`` value, or
            ``None`` for the minority of raise sites that never attach
            one (``units/fs.py``, ``units/saas/objectdb.py``).
        template: ``finding.detail`` with its instance-specific values
            placeholdered away, keeping an embedded ``[ref=...]`` tag
            visible exactly when ``ref_value`` is not ``None`` — this
            lets one field serve both ``group_findings``'s discriminator
            and a group header's display.
        variable_parts: The instance-specific values ``template``
            replaced, in order, joined for display, with ``ref_value``
            itself omitted (the header already shows it once).
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
    finding's own embedded ``[ref=...]`` value when present (an exact
    "same missing/stale object" match), or a normalized-template fallback
    otherwise. The ``"ref:"``/``"tpl:"`` prefix is cheap insurance against
    a template string ever colliding with a real ref-based key."""
    discriminator = f"ref:{aug.ref_value}" if aug.ref_value is not None else f"tpl:{aug.template}"
    return (aug.finding.stage, aug.finding.symptom.value, discriminator)


def group_findings(findings: Sequence[Finding]) -> list[list[AugmentedFinding]]:
    """Buckets ``findings`` (expected already sorted by ``sort_key``) by
    ``_group_key`` — findings resolving to the same key land in the same
    group. Groups are then sorted by that same key rather than left in
    first-member-encountered order, so two groups sharing a stage and
    symptom don't interleave by whichever member happened to sort
    chronologically first (each group's own members stay chronological
    either way).

    Returns:
        Each finding wrapped in its own ``AugmentedFinding``, so the
        ref/template/variable-parts signal is computed once here instead
        of being re-derived by each caller that renders these groups.
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
