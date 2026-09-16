"""``synology-apm-repo-cli verify <repo> [--level quick|full]`` — integrity check."""

from __future__ import annotations

import re

import typer
from rich.console import Console

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.browse import opened_repo_or_profile
from synology_apm_repo.cli.options import KeyOption, ProfileOption, RepoArgument
from synology_apm_repo.cli.progress_render import build_progress_meter
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import VERIFY_LEVEL_HELP
from synology_apm_repo.sdk.api import Finding, Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.presentation.format import pluralize
from synology_apm_repo.sdk.presentation.markup import safe

console = Console()

_Report = list[dict[str, object]]

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


def _normalize_detail(detail: str, *, keep_ref: bool = False) -> str:
    """``detail`` with every instance-specific value (see ``_VARIABLE_RE``)
    replaced by a placeholder — the "same reason, different instance"
    signature ``_group_report`` falls back to grouping findings by when no
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


def _group_key(entry: dict[str, object]) -> tuple[Stage, str, str]:
    """``(stage, symptom, discriminator)`` — ``discriminator`` is the
    finding's own embedded ``[ref=...]`` value when its detail carries one
    (an exact "same missing/stale object" match, not a resemblance), or a
    normalized-template fallback otherwise, for the minority of raise sites
    that never attach a ref (``units/fs.py``, ``units/saas/objectdb.py``).
    The ``"ref:"``/``"tpl:"`` prefix is cheap, permanent insurance against
    a template string ever colliding with a real ref-based key — not a
    real expected collision today."""
    detail = str(entry["detail"])
    ref_match = _REF_TAG_RE.search(detail)
    discriminator = f"ref:{ref_match.group(1)}" if ref_match else f"tpl:{_normalize_detail(detail)}"
    stage = entry["stage"]
    assert isinstance(stage, Stage)  # _build_report's own contract, not re-derivable from dict[str, object]'s type
    return (stage, str(entry["symptom"]), discriminator)


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


def _sort_key(f: Finding) -> tuple[int, int, str, str, str]:
    # Chronological-by-timestamp for a genuine per-version finding, so
    # this sorts by *when*, not alphabetically by workload name; the
    # whole path is the tiebreak for everything else (RepoInfo/FileMap/
    # Composition/EncryptKey, a Bucket finding not tied to one version,
    # and a Stage.VERSION finding that isn't per-version either -- see
    # _TIMESTAMP_SUFFIX_RE's own comment) -- still fully deterministic,
    # just not chronological. This is also what fixes --level full's own
    # multiprocess bucket sweep not otherwise guaranteeing an order.
    candidate = f.path.rsplit("/", 1)[-1]
    timeish = candidate if f.stage is Stage.VERSION and _TIMESTAMP_SUFFIX_RE.search(candidate) else f.path
    return (_STAGE_ORDER[f.stage], _SYMPTOM_ORDER[f.symptom.value], timeish, f.path, f.detail)


def _build_report(findings: list[Finding], *, verbose: bool) -> _Report:
    # ref (a canonical cat:/wl:/ver: NodeRef -- an internal identifier,
    # never a real display name) follows this CLI's usual
    # internal-identifier convention: --verbose-gated, like doctor's own
    # catalog_id/workload_id.
    return [
        {
            "stage": f.stage,
            "symptom": f.symptom.value,
            "path": f.path,
            "detail": f.detail,
            **({"ref": f.ref} if verbose and f.ref is not None else {}),
        }
        for f in findings
    ]


def _group_report(report: _Report) -> list[_Report]:
    """Buckets ``report`` (already sorted by ``_sort_key``) by
    ``_group_key`` — entries that resolve to the same key (the same
    ``ref``, or the same normalized-template fallback) land in the same
    group. Groups themselves are then sorted by that same key (stage,
    then symptom, then the ref/template discriminator itself) rather than
    left in first-member-encountered order: two groups sharing a stage and
    symptom otherwise interleave by whichever member happened to sort
    chronologically first, which — for a SaaS stream's own sequential
    ``ref`` values — can put e.g. ``.../1/16/saas_obj`` ahead of
    ``.../1/14/saas_obj`` merely because one workload's display name
    happens to sort before the other's. Sorting groups by their own
    ``ref``/template value directly avoids that, at the cost of no longer
    being purely chronological across groups (each group's own *members*
    stay chronological either way, unaffected by this)."""
    groups: dict[tuple[Stage, str, str], _Report] = {}
    for entry in report:
        groups.setdefault(_group_key(entry), []).append(entry)

    # Sorts the already-computed keys themselves -- each entry's
    # _group_key() ran exactly once, above; no need to recompute it per
    # group here too.
    def key_sort_key(key: tuple[Stage, str, str]) -> tuple[int, int, str]:
        stage, symptom, discriminator = key
        return (_STAGE_ORDER[stage], _SYMPTOM_ORDER[symptom], discriminator)

    return [groups[key] for key in sorted(groups, key=key_sort_key)]


def _entry_ref_suffix(entry: dict[str, object]) -> str:
    ref = entry.get("ref")
    return f" ({safe(str(ref))})" if ref is not None else ""


def _render_human(report: _Report, level: VerifyLevel) -> None:
    if not report:
        console.print(f"[green]clean[/green] — no findings at level={level.value}")
        return
    groups = _group_report(report)
    group_count = f"{len(groups)} {pluralize(len(groups), 'group')}"
    problem_count = sum(1 for entry in report if entry["symptom"] != Symptom.REPAIRED_VIA_PARITY.value)
    if problem_count:
        console.print(f"[red]{problem_count} finding(s)[/red] in {group_count} at level={level.value}")
    else:
        # Not a problem left unresolved -- see Symptom.REPAIRED_VIA_PARITY's
        # own docstring.
        console.print(
            f"[green]clean[/green] ({len(report)} self-repaired via parity) in {group_count} at level={level.value}"
        )
    # Every group -- even one with a single member -- renders the same
    # header-plus-instance-line shape: a report mixing some repeated
    # findings with a one-off is visually consistent this way, and it's
    # one rendering path instead of two.
    for group in groups:
        rep = group[0]
        rep_detail = str(rep["detail"])
        ref_match = _REF_TAG_RE.search(rep_detail)
        ref_value = ref_match.group(1) if ref_match else None
        # symptom/stage are pure structure, unescaped; path/detail/ref can
        # embed real content-derived text (a file_map path, a filename
        # near a composition offset) -- escaped at interpolation time
        # (safe()) -- see its own docstring for why.
        template = _normalize_detail(rep_detail, keep_ref=ref_value is not None)
        noun = "version" if rep["stage"] is Stage.VERSION else "occurrence"
        count_label = f"{len(group)} {pluralize(len(group), noun)}"
        console.print(f"  [{rep['symptom']}] {rep['stage']}: {safe(template)} ({count_label})")
        for entry in group:
            suffix = _variable_parts(str(entry["detail"]), ref_value=ref_value)
            line = f"    - {safe(entry['path'])}"
            if suffix:
                line += f" — {safe(suffix)}"
            console.print(line + _entry_ref_suffix(entry))


@typer_async
async def verify(
    ctx: typer.Context,
    repo: RepoArgument = None,
    key: KeyOption = None,
    level: VerifyLevel = typer.Option(VerifyLevel.QUICK.value, "--level", help=VERIFY_LEVEL_HELP),
    profile: ProfileOption = None,
) -> None:
    """Run an integrity check against REPO and report every Finding."""
    state: CliState = ctx.obj
    verify_meter = build_progress_meter(state)
    async with opened_repo_or_profile(repo, key, profile=profile, state=state) as repository:
        # verify_reachable()'s own progress runs in two stages: an
        # "items" tick while discovering/sizing every (workload, version)
        # pair's buckets, then a "bytes" tick per bucket actually checked
        # once that whole discovery pass is done and a real, stable byte
        # total is known (see units/verify_reachable.py's own docstring
        # for why checking never starts before then).
        findings = sorted(await repository.verify(level, progress=verify_meter.update), key=_sort_key)

    # Sorted once, here, on the flat Finding list -- report/--json inherit
    # that order unchanged (list order survives _build_report's list
    # comprehension), and _render_human's own grouping relies on it too
    # (see _group_report's own docstring).
    report = _build_report(findings, verbose=state.verbose)
    if state.json:
        console.print_json(data=report)
    else:
        _render_human(report, level)
