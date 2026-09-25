"""Unit tests for ``synology_apm_repo.cli.commands.verify``'s human/JSON
rendering — synthetic ``Finding`` objects covering literal Rich-markup
survival, ``Finding.ref``'s ``--verbose`` gating, and grouping/sorting of
repeated same-reason findings."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.api import Finding, Stage, Symptom

runner = CliRunner()

_SENSITIVE_PATH = "some/customer/folder/real-file-name.docx"
_SENSITIVE_DETAIL = "path points at real-file-name.docx which is missing"


@pytest.fixture(autouse=True)
def _fake_session(monkeypatch: pytest.MonkeyPatch) -> None:
    finding = Finding(
        stage=Stage.FILE_MAP, symptom=Symptom.FILE_MISSING, path=_SENSITIVE_PATH, detail=_SENSITIVE_DETAIL
    )

    class _FakeRepo:
        async def verify(self, level: object, **kwargs: object) -> list[Finding]:
            return [finding]

    class _FakeSession:
        def __init__(self) -> None:
            # Session.__init__ itself stays synchronous.
            pass

        async def open(self, *args: object, **kwargs: object) -> list[object]:
            return [_FakeRepo()]

        async def close(self) -> None:
            pass

    # verify.py doesn't import Session itself — Session() lives inside
    # cli.repo_session.opened_repo(), so that's the module this patches.
    monkeypatch.setattr("synology_apm_repo.cli.repo_session.Session", _FakeSession)


def test_json_output_shows_the_real_path(tmp_path: Path) -> None:
    result = runner.invoke(app, ["--json", "verify", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert _SENSITIVE_PATH in result.stdout
    assert _SENSITIVE_DETAIL in result.stdout


def test_human_output_shows_the_real_path(tmp_path: Path) -> None:
    result = runner.invoke(app, ["verify", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert _SENSITIVE_PATH in result.output


def test_human_output_renders_a_bracketed_path_and_detail_literally(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Real content Rich would otherwise mistake for a markup tag and
    # silently drop -- must render in full, byte-for-byte.
    bracketed_path = "[archive]/real-file-name.docx"
    bracketed_detail = "[missing] real-file-name.docx"
    finding = Finding(stage=Stage.FILE_MAP, symptom=Symptom.FILE_MISSING, path=bracketed_path, detail=bracketed_detail)

    class _FakeRepo:
        async def verify(self, level: object, **kwargs: object) -> list[Finding]:
            return [finding]

    class _FakeSession:
        def __init__(self) -> None:
            pass

        async def open(self, *args: object, **kwargs: object) -> list[object]:
            return [_FakeRepo()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr("synology_apm_repo.cli.repo_session.Session", _FakeSession)

    result = runner.invoke(app, ["verify", str(tmp_path)])
    assert result.exit_code == 0, result.output
    # Rich's own line-wrapping at the terminal width CliRunner reports can
    # split a long line across two -- collapse that back out before
    # checking, so this only pins down "brackets survive," not incidental
    # wrap placement.
    unwrapped = result.output.replace("\n", "")
    assert bracketed_path in unwrapped
    assert bracketed_detail in unwrapped


class TestRefVerboseGating:
    """``Finding.ref`` (a canonical ``cat:/wl:/ver:`` ``NodeRef`` string,
    only ``verify_reachable()`` ever sets it) follows this CLI's usual
    internal-identifier convention: hidden by default, shown under
    ``--verbose`` — the same gating ``doctor``'s own ``catalog_id``/
    ``workload_id`` already gets."""

    _REF = "/repo#cat:1/wl:2/ver:abc-uid"

    @pytest.fixture(autouse=True)
    def _fake_session_with_ref(self, monkeypatch: pytest.MonkeyPatch) -> None:
        finding = Finding(
            stage=Stage.VERSION,
            symptom=Symptom.DATA_MISSING,
            path=_SENSITIVE_PATH,
            detail=_SENSITIVE_DETAIL,
            ref=self._REF,
        )

        class _FakeRepo:
            async def verify(self, level: object, **kwargs: object) -> list[Finding]:
                return [finding]

        class _FakeSession:
            def __init__(self) -> None:
                pass

            async def open(self, *args: object, **kwargs: object) -> list[object]:
                return [_FakeRepo()]

            async def close(self) -> None:
                pass

        monkeypatch.setattr("synology_apm_repo.cli.repo_session.Session", _FakeSession)

    def test_json_output_hides_ref_without_verbose(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["--json", "verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert "ref" not in result.stdout

    def test_json_output_shows_ref_with_verbose(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["--verbose", "--json", "verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert self._REF in result.stdout

    def test_human_output_hides_ref_without_verbose(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert self._REF not in result.output

    def test_human_output_shows_ref_with_verbose(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["--verbose", "verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert self._REF in result.output


def _patch_verify_findings(monkeypatch: pytest.MonkeyPatch, findings: list[Finding]) -> None:
    class _FakeRepo:
        async def verify(self, level: object, **kwargs: object) -> list[Finding]:
            return list(findings)

    class _FakeSession:
        def __init__(self) -> None:
            pass

        async def open(self, *args: object, **kwargs: object) -> list[object]:
            return [_FakeRepo()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr("synology_apm_repo.cli.repo_session.Session", _FakeSession)


class TestGroupingAndSorting:
    """End-to-end checks that the CLI's ``verify`` command still renders
    grouped output correctly through ``sdk.dedup.verify_report``'s
    ``group_findings``/``sort_key`` — the detailed grouping-collision/
    template/ref-vs-fallback/sort-order logic itself is tested directly,
    at the SDK layer, in
    ``tests/unit/sdk/test_dedup_verify_report.py``; this class only
    covers what's genuinely CLI-specific: rendering shape, ``--json``
    output, and ``--verbose`` gating."""

    def test_repeated_findings_sharing_one_ref_collapse_into_one_group(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Smoke test that ``group_findings``'s own grouping actually
        reaches the rendered output: two versions sharing one ``ref``
        collapse into one header + one line per instance, not two
        separate headers."""
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
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        unwrapped = result.output.replace("\n", "")
        assert unwrapped.count("no file_map entry for path") == 1
        assert unwrapped.count("(2 versions)") == 1
        assert unwrapped.count("[ref=shared-obj]") == 1  # shown once, in the header -- not once per instance
        assert "wl-a/2026-01-01 00:00:01" in unwrapped
        assert "wl-b/2026-01-01 00:00:02" in unwrapped

    def test_a_non_version_stage_group_uses_occurrences_not_versions(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The count label reads "versions" only for ``Stage.VERSION`` —
        a ``Stage.BUCKET`` finding (``_open_bucket_or_finding()``'s own
        shape) isn't per-version, so grouped ``Stage.BUCKET`` findings
        keep the generic "occurrences" wording instead."""
        findings = [
            Finding(
                Stage.BUCKET,
                Symptom.DATA_MISSING,
                "bucket s1/1",
                "no such object [ref=shared-bucket] — possibly a stale/rotated version reference",
            ),
            Finding(
                Stage.BUCKET,
                Symptom.DATA_MISSING,
                "bucket s1/2",
                "no such object [ref=shared-bucket] — possibly a stale/rotated version reference",
            ),
        ]
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert "(2 occurrences)" in result.output
        assert "versions)" not in result.output

    def test_a_single_finding_still_renders_as_a_one_member_group(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """No singleton special case: a lone finding (nothing else shares
        its group) still renders as a header line (with "(1 version)")
        plus its own indented instance line — the same two-line shape
        every group uses, rather than a one-off flat single-line format
        that would look inconsistent next to any repeated finding in the
        same report."""
        findings = [Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", "no file_map entry")]
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert "(1 version)" in result.output
        assert "    - wl-a/2026-01-01 00:00:01" in result.output

    def test_json_output_stays_flat_and_sorted(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        findings = [
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-b/2026-01-01 00:00:02", "no file_map entry for path 'p2'"),
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", "no file_map entry for path 'p1'"),
        ]
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["--json", "verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        report = json.loads(result.stdout)
        assert len(report) == 2  # never collapsed, unlike human output
        assert report[0]["path"] == "wl-a/2026-01-01 00:00:01"
        assert report[1]["path"] == "wl-b/2026-01-01 00:00:02"

    def test_grouped_output_never_hides_a_ref_without_verbose(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Regression guard for the hard constraint grouping must respect:
        collapsing into a ref-based group must never make the group's own
        identifying ref value, or an individual instance's own identity,
        depend on ``--verbose``."""
        findings = [
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-a/2026-01-01 00:00:01",
                "no file_map entry for path 'shared-unique-ref' [ref=shared-unique-ref]",
            ),
            Finding(
                Stage.VERSION,
                Symptom.DATA_MISSING,
                "wl-b/2026-01-01 00:00:02",
                "no file_map entry for path 'shared-unique-ref' [ref=shared-unique-ref]",
            ),
        ]
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert "shared-unique-ref" in result.output  # the group's own ref, shown once, in the header
        assert "wl-a/2026-01-01 00:00:01" in result.output
        assert "wl-b/2026-01-01 00:00:02" in result.output


class TestRepairedViaParityRendering:
    """``Symptom.REPAIRED_VIA_PARITY`` findings are a successful repair, not
    a problem left unresolved -- ``_render_human`` must not fold them into
    the red "problem" count."""

    def test_repaired_only_report_renders_green(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        findings = [
            Finding(
                Stage.BUCKET, Symptom.REPAIRED_VIA_PARITY, "bucket 1/2", "SizeStore CRC mismatch repaired via parity"
            )
        ]
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert "clean" in result.output
        assert "1 self-repaired via parity" in result.output
        assert "finding(s)" not in result.output

    def test_a_mix_of_repaired_and_real_findings_counts_only_the_real_ones_as_red(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        findings = [
            Finding(
                Stage.BUCKET, Symptom.REPAIRED_VIA_PARITY, "bucket 1/2", "SizeStore CRC mismatch repaired via parity"
            ),
            Finding(Stage.BUCKET, Symptom.CORRUPTION, "bucket 1/3", "checksum mismatch"),
        ]
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert "1 finding(s)" in result.output  # only the real problem counted, not the repaired one too


__all__: list[str] = []
