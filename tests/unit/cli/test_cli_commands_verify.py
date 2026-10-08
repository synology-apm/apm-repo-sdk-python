"""Unit tests for ``synology_apm_repo.cli.commands.verify``'s human/JSON
report rendering."""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

import pytest
from inline_snapshot import snapshot
from rich.console import Console

import synology_apm_repo.cli.commands.verify as verify_cmd
from support.cli import invoke
from support.fakes import faithful_to
from synology_apm_repo.sdk.api import Finding, Repository, Stage, Symptom, VerifyLevel
from synology_apm_repo.sdk.presentation import ProgressCallback
from unit.cli.session_fakes import install_fake_session

_SENSITIVE_PATH = "alice/Documents/report.docx"
_SENSITIVE_DETAIL = "path points at report.docx which is missing"


@faithful_to(Repository)
class _VerifyingRepo:
    """``verify()`` returns ``findings``, recording each level it was asked for."""

    def __init__(self, findings: list[Finding]) -> None:
        self._findings = findings
        self.levels: list[VerifyLevel] = []

    async def verify(
        self, level: VerifyLevel = VerifyLevel.QUICK, *, progress: ProgressCallback | None = None
    ) -> list[Finding]:
        self.levels.append(level)
        return list(self._findings)


def _patch_verify_findings(monkeypatch: pytest.MonkeyPatch, findings: list[Finding]) -> _VerifyingRepo:
    repo = _VerifyingRepo(findings)
    install_fake_session(monkeypatch, [repo])
    return repo


@pytest.fixture(autouse=True)
def _fake_session(monkeypatch: pytest.MonkeyPatch) -> None:
    finding = Finding(
        stage=Stage.FILE_MAP, symptom=Symptom.FILE_MISSING, path=_SENSITIVE_PATH, detail=_SENSITIVE_DETAIL
    )
    _patch_verify_findings(monkeypatch, [finding])


def test_json_output_shows_the_real_path(tmp_path: Path) -> None:
    result = invoke(["--json", "verify", str(tmp_path)], exit_code=3)
    assert result.stdout == snapshot("""\
{
  "level": "quick",
  "problem_count": 1,
  "findings": [
    {
      "stage": "FileMap",
      "symptom": "FileMissing",
      "path": "alice/Documents/report.docx",
      "detail": "path points at report.docx which is missing"
    }
  ]
}
""")


def test_human_output_shows_the_real_path(tmp_path: Path) -> None:
    result = invoke(["verify", str(tmp_path)], exit_code=3)
    assert result.stdout == snapshot("""\
1 finding in 1 group at level=quick
  [FileMissing] FileMap: path points at report.docx which is missing (1 \n\
occurrence)
    - alice/Documents/report.docx
""")


def test_human_output_renders_a_bracketed_path_and_detail_literally(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bracketed_path = "[archive]/report.docx"
    bracketed_detail = "[missing] report.docx"
    finding = Finding(stage=Stage.FILE_MAP, symptom=Symptom.FILE_MISSING, path=bracketed_path, detail=bracketed_detail)

    _patch_verify_findings(monkeypatch, [finding])

    result = invoke(["verify", str(tmp_path)], exit_code=3)
    assert result.stdout == snapshot("""\
1 finding in 1 group at level=quick
  [FileMissing] FileMap: [missing] report.docx (1 occurrence)
    - [archive]/report.docx
""")


class TestRefVerboseGating:
    """``Finding.ref`` (a canonical ``cat:/wl:/ver:`` ref) is an internal
    identifier: hidden by default, shown under ``--verbose``."""

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

        _patch_verify_findings(monkeypatch, [finding])

    def test_json_output_hides_ref_without_verbose(self, tmp_path: Path) -> None:
        result = invoke(["--json", "verify", str(tmp_path)], exit_code=3)
        assert result.stdout == snapshot("""\
{
  "level": "quick",
  "problem_count": 1,
  "findings": [
    {
      "stage": "Version",
      "symptom": "DataMissing",
      "path": "alice/Documents/report.docx",
      "detail": "path points at report.docx which is missing"
    }
  ]
}
""")

    def test_json_output_shows_ref_with_verbose(self, tmp_path: Path) -> None:
        result = invoke(["--verbose", "--json", "verify", str(tmp_path)], exit_code=3)
        assert result.stdout == snapshot("""\
{
  "level": "quick",
  "problem_count": 1,
  "findings": [
    {
      "stage": "Version",
      "symptom": "DataMissing",
      "path": "alice/Documents/report.docx",
      "detail": "path points at report.docx which is missing",
      "ref": "/repo#cat:1/wl:2/ver:abc-uid"
    }
  ]
}
""")

    def test_human_output_hides_ref_without_verbose(self, tmp_path: Path) -> None:
        result = invoke(["verify", str(tmp_path)], exit_code=3)
        assert result.stdout == snapshot("""\
1 finding in 1 group at level=quick
  [DataMissing] Version: path points at report.docx which is missing (1 version)
    - alice/Documents/report.docx
""")

    def test_human_output_shows_ref_with_verbose(self, tmp_path: Path) -> None:
        result = invoke(["--verbose", "verify", str(tmp_path)], exit_code=3)
        assert result.stdout == snapshot("""\
1 finding in 1 group at level=quick
  [DataMissing] Version: path points at report.docx which is missing (1 version)
    - alice/Documents/report.docx (/repo#cat:1/wl:2/ver:abc-uid)
""")


class TestCleanReport:
    """No findings: one line on stdout, the level echoed, nothing on stderr."""

    @pytest.mark.parametrize("level", ["quick", "full"])
    def test_a_clean_repository_reports_the_level_that_was_checked(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, level: str
    ) -> None:
        repo = _patch_verify_findings(monkeypatch, [])

        result = invoke(["verify", str(tmp_path), "--level", level])
        assert result.stdout == f"clean — no findings at level={level}\n"
        assert result.stderr == ""
        assert repo.levels == [VerifyLevel(level)]


class TestGroupingAndSorting:
    """The CLI half of ``verify``'s grouping; ``sdk.presentation.verify_report``'s
    ``group_findings``/``finding_sort_key`` are tested in
    ``tests/unit/sdk/test_presentation_verify_report.py``."""

    def test_repeated_findings_sharing_one_ref_collapse_into_one_group(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
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
        result = invoke(["verify", str(tmp_path)], exit_code=3)
        # The detail and its [ref=...] show once, in the group's header, not once per instance.
        assert result.stdout == snapshot("""\
2 findings in 1 group at level=quick
  [DataMissing] Version: no file_map entry for path '…' [ref=shared-obj] — \n\
possibly a stale/rotated version reference (2 versions)
    - wl-a/2026-01-01 00:00:01
    - wl-b/2026-01-01 00:00:02
""")

    def test_a_non_version_stage_group_uses_occurrences_not_versions(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The count label reads "versions" only for ``Stage.VERSION``."""
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
        result = invoke(["verify", str(tmp_path)], exit_code=3)
        assert result.stdout == snapshot("""\
2 findings in 1 group at level=quick
  [DataMissing] Bucket: no such object [ref=shared-bucket] — possibly a \n\
stale/rotated version reference (2 occurrences)
    - bucket s1/1
    - bucket s1/2
""")

    def test_a_single_finding_still_renders_as_a_one_member_group(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        findings = [Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", "no file_map entry")]
        _patch_verify_findings(monkeypatch, findings)
        result = invoke(["verify", str(tmp_path)], exit_code=3)
        assert result.stdout == snapshot("""\
1 finding in 1 group at level=quick
  [DataMissing] Version: no file_map entry (1 version)
    - wl-a/2026-01-01 00:00:01
""")

    def test_json_output_stays_flat_and_sorted(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        findings = [
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-b/2026-01-01 00:00:02", "no file_map entry for path 'p2'"),
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", "no file_map entry for path 'p1'"),
        ]
        _patch_verify_findings(monkeypatch, findings)
        result = invoke(["--json", "verify", str(tmp_path)], exit_code=3)
        findings = json.loads(result.stdout)["findings"]
        assert len(findings) == 2  # never collapsed, unlike human output
        assert findings[0]["path"] == "wl-a/2026-01-01 00:00:01"
        assert findings[1]["path"] == "wl-b/2026-01-01 00:00:02"

    def test_grouped_output_never_hides_a_ref_without_verbose(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A group's own ref and each instance's path show without ``--verbose``."""
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
        result = invoke(["verify", str(tmp_path)], exit_code=3)
        assert result.stdout == snapshot("""\
2 findings in 1 group at level=quick
  [DataMissing] Version: no file_map entry for path '…' [ref=shared-unique-ref] \n\
(2 versions)
    - wl-a/2026-01-01 00:00:01
    - wl-b/2026-01-01 00:00:02
""")


class TestRepairedViaParityRendering:
    """``Symptom.REPAIRED_VIA_PARITY`` is a successful repair, not counted
    as a problem."""

    @staticmethod
    def _colour_stdout(monkeypatch: pytest.MonkeyPatch) -> StringIO:
        """Route the command's report through a colour terminal console (no
        pager, no automatic highlighting, so only the report's own markup is
        styled), returning the buffer its ANSI output lands in."""
        buffer = StringIO()
        monkeypatch.setenv("PAGER", "")
        monkeypatch.delenv("NO_COLOR", raising=False)  # CI sets it; Rich honours it even when forced
        monkeypatch.setattr(
            verify_cmd,
            "console",
            Console(file=buffer, force_terminal=True, color_system="standard", width=80, highlight=False),
        )
        return buffer

    def test_repaired_only_report_renders_green(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        findings = [
            Finding(
                Stage.BUCKET, Symptom.REPAIRED_VIA_PARITY, "bucket 1/2", "SizeStore CRC mismatch repaired via parity"
            )
        ]
        _patch_verify_findings(monkeypatch, findings)
        result = invoke(["verify", str(tmp_path)])
        assert result.stdout == snapshot("""\
clean (1 self-repaired via parity) in 1 group at level=quick
  [RepairedViaParity] Bucket: SizeStore CRC mismatch repaired via parity (1 \n\
occurrence)
    - bucket 1/2
""")

        coloured = self._colour_stdout(monkeypatch)
        invoke(["verify", str(tmp_path)])
        assert coloured.getvalue().startswith("\x1b[32mclean\x1b[0m (1 self-repaired via parity)")

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
        result = invoke(["verify", str(tmp_path)], exit_code=3)
        # Only the real problem is counted as a finding, not the repaired one too.
        assert result.stdout == snapshot("""\
1 finding in 2 groups at level=quick
  [Corruption] Bucket: checksum mismatch (1 occurrence)
    - bucket 1/3
  [RepairedViaParity] Bucket: SizeStore CRC mismatch repaired via parity (1 \n\
occurrence)
    - bucket 1/2
""")

        coloured = self._colour_stdout(monkeypatch)
        invoke(["verify", str(tmp_path)], exit_code=3)
        assert coloured.getvalue().startswith("\x1b[31m1 finding\x1b[0m in 2 groups")
