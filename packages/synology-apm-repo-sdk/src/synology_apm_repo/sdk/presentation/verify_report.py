"""Grouping and a stable render order for a batch of ``Finding``\\ s from one
``Repository.verify()``/``Catalog.verify()`` run: turns a flat list into
"same root cause" groups, shared by the CLI's ``verify`` command and the
TUI's diagnostics screen.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Iterable, Sequence

from ..findings import Finding, Stage, Symptom
from .format import pluralize

_STAGE_ORDER = {stage: i for i, stage in enumerate(Stage)}
_SYMPTOM_ORDER = {symptom.value: i for i, symptom in enumerate(Symptom)}

#: An instance-specific value a ``Finding.detail`` sentence embeds: a
#: quoted path/name, a bracketed ``[ref=...]``/``[spec=...]`` tag, or a
#: bare run of digits. Quoted/bracketed spans are tried first so a digit
#: run inside one isn't matched again. The quoted span honors backslash
#: escapes, since ``repr()`` can escape an inner ``'``.
_VARIABLE_RE = re.compile(r"'(?:[^'\\]|\\.)*'|\[(?:ref|spec)=[^\]]*\]|\d+")

#: The value half of an embedded ``[ref=...]`` tag: it names the exact
#: missing/stale object, a more precise "same root cause" signal than the
#: regex template (see ``_group_key``).
_REF_TAG_RE = re.compile(r"\[ref=([^\]]*)\]")

#: A per-version ``Stage.VERSION`` finding's path ends in
#: ``Version.display_name``'s ``"YYYY-MM-DD HH:MM:SS"``; connection/workload-
#: enumeration and PC/PS per-disk findings use labels that do not, which
#: this shape tells apart.
_TIMESTAMP_SUFFIX_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


def _normalize_detail(detail: str, *, keep_ref: bool = False) -> str:
    """``detail`` with every instance-specific value (see ``_VARIABLE_RE``)
    replaced by a placeholder: the "same reason, different instance"
    signature used when no ``ref`` is available (see ``_group_key``).
    ``keep_ref=True`` leaves an embedded ``[ref=...]`` tag as written."""

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
    order, joined for display. ``ref_value``, when given, omits its
    ``[ref=...]`` tag and any quoted copy of it (the header shows it)."""
    parts = [
        match.group(0)
        for match in _VARIABLE_RE.finditer(detail)
        if ref_value is None or match.group(0) not in (f"[ref={ref_value}]", f"'{ref_value}'")
    ]
    return " ".join(parts)


def finding_sort_key(finding: Finding) -> tuple[int, int, str, str, str]:
    """A stable, deterministic order for a batch of ``Finding``\\ s:
    chronological (by the version-display-name timestamp in ``path``) for a
    per-version finding, alphabetical by whole ``path`` otherwise. This makes
    the order of a ``VerifyLevel.FULL`` multiprocess sweep deterministic."""
    candidate = finding.path.rsplit("/", 1)[-1]
    timeish = candidate if finding.stage is Stage.VERSION and _TIMESTAMP_SUFFIX_RE.search(candidate) else finding.path
    return (_STAGE_ORDER[finding.stage], _SYMPTOM_ORDER[finding.symptom.value], timeish, finding.path, finding.detail)


@dataclasses.dataclass(frozen=True, slots=True)
class AugmentedFinding:
    """One ``Finding`` plus the ref/template/variable-parts signal that
    grouping and rendering share.

    Attributes:
        finding: The underlying ``Finding``.
        ref_value: This finding's own embedded ``[ref=...]`` value, or
            ``None`` when the detail carries none.
        template: ``finding.detail`` with its instance-specific values
            placeholdered away, keeping an embedded ``[ref=...]`` tag
            visible exactly when ``ref_value`` is not ``None``.
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
    """``(stage, symptom, discriminator)``: the discriminator is the embedded
    ``[ref=...]`` value when present, else the normalized template; the
    ``"ref:"``/``"tpl:"`` prefix keeps the two key spaces apart."""
    discriminator = f"ref:{aug.ref_value}" if aug.ref_value is not None else f"tpl:{aug.template}"
    return (aug.finding.stage, aug.finding.symptom.value, discriminator)


def group_findings(findings: Sequence[Finding]) -> list[list[AugmentedFinding]]:
    """Buckets ``findings`` by ``_group_key``. Expects them already sorted by
    ``finding_sort_key`` (``summarize_findings`` does that), which keeps each group's
    members in that order. Groups themselves are sorted by key, not by first
    member.

    Returns:
        The groups, each finding wrapped in an ``AugmentedFinding``.
    """
    groups: dict[tuple[Stage, str, str], list[AugmentedFinding]] = {}
    for finding in findings:
        aug = _augment(finding)
        groups.setdefault(_group_key(aug), []).append(aug)

    def key_sort_key(key: tuple[Stage, str, str]) -> tuple[int, int, str]:
        stage, symptom, discriminator = key
        return (_STAGE_ORDER[stage], _SYMPTOM_ORDER[symptom], discriminator)

    return [groups[key] for key in sorted(groups, key=key_sort_key)]


def group_count_label(group: Sequence[AugmentedFinding]) -> str:
    """``"<N> version(s)"`` for a ``Stage.VERSION`` group, else
    ``"<N> occurrence(s)"``: the member-count label under a group header."""
    noun = "version" if group[0].finding.stage is Stage.VERSION else "occurrence"
    return f"{len(group)} {pluralize(len(group), noun)}"


@dataclasses.dataclass(frozen=True, slots=True)
class VerifySummary:
    """One verify run's findings, sorted, grouped and counted.

    Attributes:
        findings: Every finding, sorted by ``finding_sort_key``.
        groups: ``group_findings(findings)``.
        problem_count: Findings still unresolved: every finding except a
            ``Symptom.REPAIRED_VIA_PARITY`` one (a CRC mismatch the SDK's
            redundancy-blob self-repair already reconstructed and confirmed).
        repaired_count: Findings that were ``Symptom.REPAIRED_VIA_PARITY``.
    """

    findings: list[Finding]
    groups: list[list[AugmentedFinding]]
    problem_count: int
    repaired_count: int

    @property
    def group_count(self) -> int:
        return len(self.groups)

    def headline(self, level: str) -> str | None:
        """The result line the CLI and the Browser both show above the
        groups, as Rich markup, for a run at verify level ``level``;
        ``None`` when there are no findings at all, which each frontend
        words itself."""
        if not self.findings:
            return None
        groups = f"{self.group_count} {pluralize(self.group_count, 'group')}"
        if self.problem_count:
            findings = f"{self.problem_count} {pluralize(self.problem_count, 'finding')}"
            return f"[red]{findings}[/red] in {groups} at level={level}"
        return f"[green]clean[/green] ({self.repaired_count} self-repaired via parity) in {groups} at level={level}"


def summarize_findings(findings: Iterable[Finding]) -> VerifySummary:
    """Sorts ``findings`` by ``finding_sort_key`` and returns them with their groups
    and problem/repaired counts; the entry point every frontend renders a
    verify result from."""
    ordered = sorted(findings, key=finding_sort_key)
    repaired = sum(1 for f in ordered if f.symptom is Symptom.REPAIRED_VIA_PARITY)
    return VerifySummary(
        findings=ordered,
        groups=group_findings(ordered),
        problem_count=len(ordered) - repaired,
        repaired_count=repaired,
    )
