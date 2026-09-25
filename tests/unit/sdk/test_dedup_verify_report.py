"""Unit tests for ``synology_apm_repo.sdk.dedup.verify_report`` — grouping
repeated ``Finding``\\ s that share a root cause, and the stable render
order ``sort_key`` gives a batch of them. This lives at the SDK layer,
next to ``Finding`` itself, since grouping/ordering are structural
concerns, not CLI-presentation ones; ``tests/unit/cli/test_cli_verify.py``
keeps thinner end-to-end checks that the CLI still renders grouped output
correctly, not a second copy of every case below."""

from __future__ import annotations

import pytest

from synology_apm_repo.sdk.api import Finding, Stage, Symptom
from synology_apm_repo.sdk.dedup import verify_report
from synology_apm_repo.sdk.dedup.verify_report import group_findings, sort_key


class TestGroupFindings:
    def test_repeated_findings_sharing_one_ref_collapse_into_one_group(self) -> None:
        """Two versions whose detail names the exact same missing/stale
        object (``[ref=shared-obj]``, identical in both) collapse into one
        group — grouping's primary key. The group's own ``ref_value``/
        ``template`` carry the shared signal once; each member still keeps
        its own ``finding``."""
        findings = [
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-a/2026-01-01 00:00:01",
                "no file_map entry for path 'shared-obj' [ref=shared-obj] — possibly a stale/rotated version reference",
            ),
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-b/2026-01-01 00:00:02",
                "no file_map entry for path 'shared-obj' [ref=shared-obj] — possibly a stale/rotated version reference",
            ),
        ]
        groups = group_findings(sorted(findings, key=sort_key))
        assert len(groups) == 1
        assert len(groups[0]) == 2
        assert groups[0][0].ref_value == "shared-obj"
        assert {aug.finding.path for aug in groups[0]} == {"wl-a/2026-01-01 00:00:01", "wl-b/2026-01-01 00:00:02"}

    def test_a_spec_tag_stays_placeholdered_even_in_a_ref_based_group(self) -> None:
        """``keep_ref=True`` only exempts the ``ref`` tag specifically —
        the group's own ``template`` still abstracts a separate
        ``[spec=...]`` tag (``ApmRepoError``'s other optional forensics
        field) away to a bare ``[spec]`` placeholder, since nothing
        establishes it's constant across the group the way ``ref`` itself
        is by construction; the real per-instance value still shows in
        each member's own ``variable_parts``."""
        findings = [
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-a/2026-01-01 00:00:01",
                "no file_map entry for path 'shared-obj' [ref=shared-obj] [spec=FORMAT-SPEC.md: A]",
            ),
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-b/2026-01-01 00:00:02",
                "no file_map entry for path 'shared-obj' [ref=shared-obj] [spec=FORMAT-SPEC.md: B]",
            ),
        ]
        groups = group_findings(sorted(findings, key=sort_key))
        assert len(groups) == 1
        template = groups[0][0].template
        assert "[ref=shared-obj]" in template  # the ref tag: verbatim
        assert "[spec]" in template  # the spec tag: still placeholdered
        assert "FORMAT-SPEC.md" not in template  # the template itself never leaks a concrete spec value
        variable_parts_by_path = {aug.finding.path: aug.variable_parts for aug in groups[0]}
        assert "FORMAT-SPEC.md: A" in variable_parts_by_path["wl-a/2026-01-01 00:00:01"]
        assert "FORMAT-SPEC.md: B" in variable_parts_by_path["wl-b/2026-01-01 00:00:02"]

    def test_a_quoted_value_with_both_quote_characters_is_matched_as_one_span(self) -> None:
        """A raise site formatting a real path via ``{path!r}`` can embed
        Python's own backslash-escaped ``repr()`` output — e.g. a real
        filename containing both a ``'`` and a ``"`` forces ``repr()`` to
        keep ``'`` as the delimiter and escape the internal one
        (``repr("O'Brien's \\"backup\\".pst")`` is
        ``'O\\'Brien\\'s "backup".pst'``). A non-escape-aware quoted-span
        pattern stops at that first escaped ``'`` instead of the real
        closing one, leaking a mangled fragment (``Brien\\'``) straight
        into the group's own ``template``."""
        path = repr("O'Brien's \"backup\".pst")
        findings = [
            Finding(
                Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", f"no file_map entry for path {path}"
            ),
        ]
        groups = group_findings(sorted(findings, key=sort_key))
        assert "Brien" not in groups[0][0].template

    def test_findings_sharing_a_ref_but_different_templates_still_merge(self) -> None:
        """``ref`` is the primary grouping key — two findings naming the
        same missing object merge even though their surrounding sentence
        differs, since it's the same underlying root cause either way."""
        findings = [
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-a/2026-01-01 00:00:01",
                "no version_info row for snapshot_id=2 version_id=1 [ref=@X] — possibly a stale/rotated reference",
            ),
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-b/2026-01-01 00:00:02",
                "no version_info row for snapshot_id=1 version_id=1 [ref=@X] — possibly a stale/rotated reference",
            ),
        ]
        groups = group_findings(sorted(findings, key=sort_key))
        assert len(groups) == 1
        assert len(groups[0]) == 2

    def test_findings_with_the_same_template_but_different_refs_do_not_merge(self) -> None:
        """Same sentence shape, but a different ``ref`` each — two
        genuinely distinct missing objects, so they must render as two
        separate one-member groups, not merge into one."""
        findings = [
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-a/2026-01-01 00:00:01",
                "no file_map entry for path 'p1' [ref=p1] — possibly a stale/rotated version reference",
            ),
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-b/2026-01-01 00:00:02",
                "no file_map entry for path 'p2' [ref=p2] — possibly a stale/rotated version reference",
            ),
        ]
        groups = group_findings(sorted(findings, key=sort_key))
        assert len(groups) == 2
        assert all(len(group) == 1 for group in groups)

    def test_findings_with_no_ref_at_all_still_collapse_via_template_fallback(self) -> None:
        """Not every raise site attaches a ``ref`` (``units/saas/objectdb.py``'s
        own ``NotFoundError`` doesn't) — grouping must still fall back to
        the normalized-template match for these."""
        findings = [
            Finding(
                Stage.VERSION,
                Symptom.CORRUPTION,
                "wl-a/2026-01-01 00:00:01",
                "no object_table row for object_id=42",
            ),
            Finding(
                Stage.VERSION,
                Symptom.CORRUPTION,
                "wl-b/2026-01-01 00:00:02",
                "no object_table row for object_id=43",
            ),
        ]
        groups = group_findings(sorted(findings, key=sort_key))
        assert len(groups) == 1
        assert groups[0][0].ref_value is None
        assert groups[0][0].template == "no object_table row for object_id=#"

    def test_a_single_finding_still_renders_as_a_one_member_group(self) -> None:
        findings = [Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", "no file_map entry")]
        groups = group_findings(sorted(findings, key=sort_key))
        assert len(groups) == 1
        assert len(groups[0]) == 1

    def test_findings_with_different_templates_do_not_merge(self) -> None:
        findings = [
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", "no file_map entry for path"),
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-b/2026-01-01 00:00:02", "no such object"),
        ]
        groups = group_findings(sorted(findings, key=sort_key))
        assert len(groups) == 2

    def test_findings_within_one_group_are_sorted_chronologically_regardless_of_discovery_order(self) -> None:
        """A group's own members still render chronologically, fed out of
        order — ``sort_key``'s own job, unaffected by ``group_findings``'s
        own inter-group ordering (see the next test)."""
        findings = [
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-b/2026-03-01 00:00:00",
                "no file_map entry for path 'shared' [ref=shared]",
            ),
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-a/2026-01-01 00:00:00",
                "no file_map entry for path 'shared' [ref=shared]",
            ),
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-c/2026-02-01 00:00:00",
                "no file_map entry for path 'shared' [ref=shared]",
            ),
        ]
        groups = group_findings(sorted(findings, key=sort_key))
        assert len(groups) == 1
        assert [aug.finding.path for aug in groups[0]] == [
            "wl-a/2026-01-01 00:00:00",
            "wl-c/2026-02-01 00:00:00",
            "wl-b/2026-03-01 00:00:00",
        ]

    def test_groups_are_sorted_by_their_own_ref_not_by_whichever_member_is_chronologically_first(self) -> None:
        """Two separate ref-groups order by their own ref value, not by
        which one's earliest member happens to sort chronologically first
        — the real shape that motivated this: two SaaS refs interleaving
        oddly in a report because one workload's display name happens to
        sort before the other's on the day both first appear. Ref "b"'s
        only member (2026-01-01) is chronologically earlier than ref
        "a"'s (2026-01-02) — first-member ordering would put "b" first;
        ref-based ordering must still put "a" first."""
        findings = [
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-x/2026-01-01 00:00:00",
                "no file_map entry for path 'b' [ref=b]",
            ),
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-y/2026-01-02 00:00:00",
                "no file_map entry for path 'a' [ref=a]",
            ),
        ]
        groups = group_findings(sorted(findings, key=sort_key))
        assert [group[0].ref_value for group in groups] == ["a", "b"]

    def test_computed_once_per_finding_not_once_per_consumer(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Locks in ``AugmentedFinding``'s own reason for existing: a
        naive grouping implementation re-derives ``ref``/``template``/
        ``variable_parts`` once during grouping and again per group
        representative at render time. ``_augment`` is the only call site
        for ``_normalize_detail``/``_variable_parts``, so proving it runs
        exactly once per finding proves both stay computed once,
        structurally — a future edit reintroducing the duplication would
        make this assertion fail, not just happen to still pass."""
        calls: list[Finding] = []
        real_augment = verify_report._augment

        def _counting_augment(finding: Finding) -> verify_report.AugmentedFinding:
            calls.append(finding)
            return real_augment(finding)

        monkeypatch.setattr(verify_report, "_augment", _counting_augment)
        findings = [
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", "detail one '123'"),
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-b/2026-01-01 00:00:02", "detail two '456'"),
        ]
        group_findings(sorted(findings, key=sort_key))
        assert len(calls) == len(findings)


class TestSortKey:
    def test_a_non_per_version_stage_version_finding_sorts_by_whole_path_not_a_bogus_timestamp(self) -> None:
        """Not every ``Stage.VERSION`` finding is per-version — a
        connection-enumeration failure (``units/verify_reachable.py``'s
        ``_unresolvable_finding(repo.layout.repo_root, exc)``) uses a bare
        filesystem path, with no ``"<workload>/<version>"`` shape and no
        trailing timestamp at all. These two findings share the same
        stage+symptom, so the sort depends entirely on ``sort_key``'s
        tiebreak, and the two candidate strategies disagree on the
        result: naively trusting the last ``/``-separated segment
        ("root") sorts *after* a real timestamp (``"r" > "2"``), while
        falling back to the whole path ("/repo/root") sorts *before* one
        (``"/" < "2"``) — this pins down the latter, correct behavior."""
        per_version = Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:00", "reason A")
        enumeration_failure = Finding(Stage.VERSION, Symptom.DATA_MISSING, "/repo/root", "connection enum failed")
        assert sorted([per_version, enumeration_failure], key=sort_key) == [enumeration_failure, per_version]
