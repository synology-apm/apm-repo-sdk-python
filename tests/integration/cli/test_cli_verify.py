"""Regression test for ``synology-apm-repo-cli verify`` — replayed from committed
fixtures recorded against real bytes, with **no external dependency** —
same ``patch_profile_store`` fixture (``tests/conftest.py``) every
sibling in this directory uses.

Fixture: ``cli_verify_sample1.json.gz``, this file's own dedicated
recording against ``sample-1`` — a small (~12 MB), 2-catalog SaaS
repository, chosen specifically for its size: ``verify_reachable()``'s
``VerifyLevel.FULL`` (see ``units/verify_reachable.py``) exhaustively
decrypts+decompresses+fingerprints every reachable chunk with no sampling
cap, so a FULL-level recording's size is proportional to the real
repository's own reachable data — ``apv-sample-1`` (8+ GB) is far too
large to record at FULL under this design. Recorded with
``allow_content=True`` (FULL genuinely decodes every reachable chunk as a
structural CRC/fingerprint oracle, never asserting on a chunk's own
content meaning — see ``tests/CLAUDE.md``'s content-recording guard).

sample-1 carries 2 real, pre-existing GW versions whose own
``saas_snapshot`` has no ``version_info`` row at all for their
``(snapshot_id, version_id)`` — a genuine gap (the stream's own metadata
never recorded these versions, not a superseded generation
``SaasStream.open_saas_obj``'s forward resolution could substitute for —
see ``units/saas/stream.py``'s own module docstring), so sample-1 is
*not* clean: it always reports exactly these same 2 findings, at both
levels, with nothing on top of them. Both share one ``ref`` (one
stream), so they collapse to a single group in the CLI's own grouped
human output. ``test_full_level_flag_replayed`` (a strict superset of
what ``VerifyLevel.QUICK`` touches) is this fixture's recording recipe.

Every scenario sharing this fixture passes ``allow_content=True`` since
its recording recipe (``test_full_level_flag_replayed``) is FULL, which
genuinely decodes real chunk content — ``VerifyLevel.QUICK`` itself reads
no chunk content at all (see ``units/verify_reachable.py``'s own
docstring) and needs no such guard on its own, but shares this fixture's
name/recording session with the FULL scenario above.
"""

from __future__ import annotations

import json
from collections.abc import Callable

from typer.testing import CliRunner

import synology_apm_repo.cli.browse as browse_mod
from synology_apm_repo.cli.main import app

runner = CliRunner()

#: sample-1's real key — see ``tests/CLAUDE.md``'s "Recording a fixture"
#: section for why this literal is safe to commit.
_SAMPLE1_KEY_STRING = "wLeLZp9tnAYw@s9m9JIplgBRHN4IPJ+75W8ttZ5okHyFjswEYwGc1K+o="

#: The known-gap findings sample-1 always reports -- 2 real, pre-existing
#: GW versions with no version_info row at all, per this module's own
#: docstring. Not the individual ``path``/``detail`` strings themselves
#: (those carry catalog-metadata-anonymized display names --
#: tests/CLAUDE.md's own "never hardcode an anonymized display-name
#: string" rule), only the shape every one of them shares.
_EXPECTED_FINDING_COUNT = 2

#: Those 2 findings share one identical ``ref`` (one stream), so they
#: collapse to a single group once ``verify``'s human output groups them
#: by that value. A structural fact about this fixture, not an
#: anonymized value, so safe to hardcode alongside
#: ``_EXPECTED_FINDING_COUNT`` above.
_EXPECTED_TEMPLATE_COUNT = 1

#: The wording ``discover_version`` gives a GW/M365 ``NotFoundError`` once
#: SaasStream.open_saas_obj's own forward resolution has already ruled
#: out routine generation rotation -- see verify_reachable.py's own
#: ``_SAAS_GENUINE_GAP_SUFFIX``.
_SAAS_GENUINE_GAP_TEXT = "genuine SaaS resolution gap"


def _assert_expected_genuine_gap_findings(findings: list[dict[str, str]]) -> None:
    assert len(findings) == _EXPECTED_FINDING_COUNT
    assert all(f["stage"] == "Version" for f in findings)
    assert all(f["symptom"] == "DataMissing" for f in findings)
    assert all(_SAAS_GENUINE_GAP_TEXT in f["detail"] for f in findings)


def test_quick_level_reports_known_genuine_gap_findings_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_verify_sample1.json.gz", browse_mod, allow_content=True)
    result = runner.invoke(app, ["--json", "verify", "--profile", "anything", "--key", _SAMPLE1_KEY_STRING])
    assert result.exit_code == 0, result.output
    _assert_expected_genuine_gap_findings(json.loads(result.stdout))


def test_full_level_flag_replayed(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_verify_sample1.json.gz", browse_mod, allow_content=True)
    result = runner.invoke(
        app,
        ["--json", "verify", "--profile", "anything", "--key", _SAMPLE1_KEY_STRING, "--level", "full"],
    )
    assert result.exit_code == 0, result.output
    _assert_expected_genuine_gap_findings(json.loads(result.stdout))


def test_human_output_reports_the_same_findings_replayed(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_verify_sample1.json.gz", browse_mod, allow_content=True)
    json_result = runner.invoke(app, ["--json", "verify", "--profile", "anything", "--key", _SAMPLE1_KEY_STRING])
    assert json_result.exit_code == 0, json_result.output
    findings = json.loads(json_result.stdout)
    _assert_expected_genuine_gap_findings(findings)

    result = runner.invoke(app, ["verify", "--profile", "anything", "--key", _SAMPLE1_KEY_STRING])
    assert result.exit_code == 0, result.output
    assert "level=quick" in result.output
    # Rich wraps long lines at the terminal width CliRunner reports --
    # collapse that back out before counting/searching, so this only pins
    # down content, not incidental wrap placement (same technique
    # test_cli_verify.py's own bracket-literal test uses).
    unwrapped = result.output.replace("\n", "")
    # Grouping collapses the shared ref's own sentence down to one header
    # line (with its own "(N versions)" annotation, both findings being
    # Stage.VERSION) instead of one full line per finding -- see
    # _EXPECTED_TEMPLATE_COUNT's own comment for why 1, not 2, is the
    # right count here.
    assert unwrapped.count(_SAAS_GENUINE_GAP_TEXT) == _EXPECTED_TEMPLATE_COUNT
    assert unwrapped.count("versions)") == _EXPECTED_TEMPLATE_COUNT
    # Every finding's own path (an anonymized display name, fetched fresh
    # from --json above rather than hardcoded -- tests/CLAUDE.md's own
    # rule) must still appear somewhere in the grouped rendering: grouping
    # only collapses the shared sentence, never an individual instance's
    # own identity.
    for finding in findings:
        assert finding["path"] in unwrapped


def test_clean_repo_reports_no_findings_replayed(patch_profile_store: Callable[..., None]) -> None:
    """A genuinely clean real repository must still report zero findings
    at QUICK — the one real-byte regression the sample-1 fixtures above
    can't cover, since every scenario against them always expects the
    same 2 known, pre-existing findings by design. Without a real, clean
    repository in the mix, a regression in ``verify_reachable()``'s own
    check logic (spuriously flagging normal, uncorrupted data) would
    pass every other test in this file silently. Its own dedicated
    fixture, ``cli_verify_apv1_vault_clean.json.gz``, is recorded against
    ``apv-sample-1`` (unencrypted, no ``--key`` needed). No
    ``allow_content=True`` here — QUICK reads no chunk content at all, so
    recording this fixture without it is itself a structural proof
    (enforced by ``ContentRecordingBlocked``) that QUICK genuinely touches
    none."""
    patch_profile_store("cli_verify_apv1_vault_clean.json.gz", browse_mod)
    result = runner.invoke(app, ["--json", "verify", "--profile", "anything"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == []


__all__: list[str] = []
