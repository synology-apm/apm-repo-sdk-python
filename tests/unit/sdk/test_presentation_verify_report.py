"""Unit tests for ``synology_apm_repo.sdk.presentation.verify_report`` — grouping
repeated ``Finding``\\ s that share a root cause, the stable order
``finding_sort_key`` gives a batch of them, and ``summarize_findings`` with
its summary's ``headline``."""

from __future__ import annotations

import pytest
from inline_snapshot import snapshot

from synology_apm_repo.sdk.findings import Finding, Stage, Symptom
from synology_apm_repo.sdk.presentation import verify_report
from synology_apm_repo.sdk.presentation.verify_report import finding_sort_key, group_findings, summarize_findings


class TestGroupFindings:
    def test_repeated_findings_sharing_one_ref_collapse_into_one_group(self) -> None:
        """Findings naming the same ``[ref=...]`` collapse into one group
        (``ref`` is the primary grouping key); each member keeps its own ``finding``."""
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
        groups = group_findings(sorted(findings, key=finding_sort_key))
        assert len(groups) == 1
        assert len(groups[0]) == 2
        assert groups[0][0].ref_value == "shared-obj"
        assert {aug.finding.path for aug in groups[0]} == {"wl-a/2026-01-01 00:00:01", "wl-b/2026-01-01 00:00:02"}

    def test_a_spec_tag_stays_placeholdered_even_in_a_ref_based_group(self) -> None:
        """Only the ``ref`` tag is kept verbatim in a group's ``template``; a
        ``[spec=...]`` tag becomes a ``[spec]`` placeholder, and its real
        value stays in each member's ``variable_parts``."""
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
        groups = group_findings(sorted(findings, key=finding_sort_key))
        assert len(groups) == 1
        assert groups[0][0].template == snapshot("no file_map entry for path '…' [ref=shared-obj] [spec]")
        assert {aug.finding.path: aug.variable_parts for aug in groups[0]} == snapshot(
            {
                "wl-a/2026-01-01 00:00:01": "[spec=FORMAT-SPEC.md: A]",
                "wl-b/2026-01-01 00:00:02": "[spec=FORMAT-SPEC.md: B]",
            }
        )

    def test_a_quoted_value_with_both_quote_characters_is_matched_as_one_span(self) -> None:
        """A ``repr()`` containing both quote characters (backslash-escaped
        inner ``'``) is normalized as one quoted span, leaving no fragment
        of it in the ``template``."""
        path = repr("O'Brien's \"backup\".pst")
        findings = [
            Finding(
                Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", f"no file_map entry for path {path}"
            ),
        ]
        groups = group_findings(sorted(findings, key=finding_sort_key))
        assert groups[0][0].template == snapshot("no file_map entry for path '…'")

    def test_findings_sharing_a_ref_but_different_templates_still_merge(self) -> None:
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
        groups = group_findings(sorted(findings, key=finding_sort_key))
        assert len(groups) == 1
        assert len(groups[0]) == 2

    def test_findings_with_the_same_template_but_different_refs_do_not_merge(self) -> None:
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
        groups = group_findings(sorted(findings, key=finding_sort_key))
        assert len(groups) == 2
        assert all(len(group) == 1 for group in groups)

    def test_findings_with_no_ref_at_all_still_collapse_via_template_fallback(self) -> None:
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
        groups = group_findings(sorted(findings, key=finding_sort_key))
        assert len(groups) == 1
        assert groups[0][0].ref_value is None
        assert groups[0][0].template == "no object_table row for object_id=#"

    def test_a_single_finding_still_renders_as_a_one_member_group(self) -> None:
        findings = [Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", "no file_map entry")]
        groups = group_findings(sorted(findings, key=finding_sort_key))
        assert len(groups) == 1
        assert len(groups[0]) == 1

    def test_findings_with_different_templates_do_not_merge(self) -> None:
        findings = [
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", "no file_map entry for path"),
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-b/2026-01-01 00:00:02", "no such object"),
        ]
        groups = group_findings(sorted(findings, key=finding_sort_key))
        assert len(groups) == 2

    def test_findings_within_one_group_are_sorted_chronologically_regardless_of_discovery_order(self) -> None:
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
        groups = group_findings(sorted(findings, key=finding_sort_key))
        assert len(groups) == 1
        assert [aug.finding.path for aug in groups[0]] == [
            "wl-a/2026-01-01 00:00:00",
            "wl-c/2026-02-01 00:00:00",
            "wl-b/2026-03-01 00:00:00",
        ]

    def test_groups_are_sorted_by_their_own_ref_not_by_whichever_member_is_chronologically_first(self) -> None:
        """Groups order by their own ref value, not by their earliest member:
        ref "b"'s member is older than ref "a"'s, yet "a" comes first."""
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
        groups = group_findings(sorted(findings, key=finding_sort_key))
        assert [group[0].ref_value for group in groups] == ["a", "b"]

    def test_computed_once_per_finding_not_once_per_consumer(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``_augment`` runs exactly once per finding."""
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
        group_findings(sorted(findings, key=finding_sort_key))
        assert len(calls) == len(findings)


class TestSortKey:
    def test_a_non_per_version_stage_version_finding_sorts_by_whole_path_not_a_bogus_timestamp(self) -> None:
        """A ``Stage.VERSION`` finding whose path has no ``<workload>/<version>``
        shape (a connection-enumeration failure on a bare repo path) sorts by
        its whole path, so it precedes a real timestamped path."""
        per_version = Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:00", "reason A")
        enumeration_failure = Finding(Stage.VERSION, Symptom.DATA_MISSING, "/repo/root", "connection enum failed")
        assert sorted([per_version, enumeration_failure], key=finding_sort_key) == [enumeration_failure, per_version]


class TestSummarizeFindings:
    def test_sorts_groups_and_counts_in_one_call(self) -> None:
        late = Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl/2026-02-02 00:00:00", "no entry for 'a' [ref=a]")
        early = Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl/2026-01-01 00:00:00", "no entry for 'a' [ref=a]")
        other = Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl/2026-03-03 00:00:00", "no entry for 'b' [ref=b]")

        summary = summarize_findings([other, late, early])  # deliberately unsorted

        assert summary.findings == sorted([other, late, early], key=finding_sort_key)
        assert summary.findings[:2] == [early, late]
        assert summary.group_count == 2
        assert [len(group) for group in summary.groups] == [2, 1]
        assert (summary.problem_count, summary.repaired_count) == (3, 0)

    def test_a_self_repaired_finding_is_counted_as_repaired_not_as_a_problem(self) -> None:
        repaired = Finding(Stage.BUCKET, Symptom.REPAIRED_VIA_PARITY, "pool/0.buk", "chunk 3 repaired from parity")
        broken = Finding(Stage.BUCKET, Symptom.DATA_MISSING, "pool/1.buk", "bucket file is missing")

        summary = summarize_findings([repaired, broken])

        assert (summary.problem_count, summary.repaired_count) == (1, 1)
        assert len(summary.findings) == 2

    def test_only_self_repaired_findings_leave_no_problem(self) -> None:
        repaired = Finding(Stage.BUCKET, Symptom.REPAIRED_VIA_PARITY, "pool/0.buk", "chunk 3 repaired from parity")

        summary = summarize_findings([repaired])

        assert (summary.problem_count, summary.repaired_count, summary.group_count) == (0, 1, 1)

    def test_no_findings_is_an_empty_summary(self) -> None:
        summary = summarize_findings([])

        assert summary.findings == []
        assert summary.groups == []
        assert (summary.problem_count, summary.repaired_count, summary.group_count) == (0, 0, 0)

    def test_accepts_any_iterable(self) -> None:
        finding = Finding(Stage.BUCKET, Symptom.DATA_MISSING, "pool/1.buk", "bucket file is missing")

        assert summarize_findings(iter([finding])).findings == [finding]


class TestHeadline:
    """The one result line the CLI and the Browser share."""

    @staticmethod
    def _finding(symptom: Symptom, path: str) -> Finding:
        return Finding(Stage.BUCKET, symptom, path, "detail")

    def test_no_findings_has_no_headline(self) -> None:
        assert summarize_findings([]).headline("quick") is None

    def test_problems_are_counted_red_and_pluralized(self) -> None:
        one = summarize_findings([self._finding(Symptom.CORRUPTION, "b/1")])
        two = summarize_findings([self._finding(Symptom.CORRUPTION, "b/1"), self._finding(Symptom.MISMATCH, "b/2")])

        assert one.headline("full") == "[red]1 finding[/red] in 1 group at level=full"
        assert two.headline("full") == "[red]2 findings[/red] in 2 groups at level=full"

    def test_only_self_repaired_findings_read_as_clean(self) -> None:
        summary = summarize_findings([self._finding(Symptom.REPAIRED_VIA_PARITY, "b/1")])

        assert (
            summary.headline("quick") == "[green]clean[/green] (1 self-repaired via parity) in 1 group at level=quick"
        )
