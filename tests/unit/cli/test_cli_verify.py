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
    # cli.browse.opened_repo(), so that's the module this patches.
    monkeypatch.setattr("synology_apm_repo.cli.browse.Session", _FakeSession)


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

    monkeypatch.setattr("synology_apm_repo.cli.browse.Session", _FakeSession)

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

        monkeypatch.setattr("synology_apm_repo.cli.browse.Session", _FakeSession)

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

    monkeypatch.setattr("synology_apm_repo.cli.browse.Session", _FakeSession)


class TestGroupingAndSorting:
    """``_group_report``/``_sort_key`` (``commands/verify.py``): repeated
    findings that share a root cause collapse into one header + one line
    per instance, and findings render in a stable, chronological order
    regardless of discovery order — see each test's own docstring for the
    specific shape it pins down."""

    def test_repeated_findings_sharing_one_ref_collapse_into_one_group(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Two versions whose detail names the exact same missing/stale
        object (``[ref=shared-obj]``, identical in both) collapse into
        one group — grouping's primary key, per Part D.1. The header
        shows that concrete ref once; per-instance lines no longer repeat
        it (it's the same value for every member, by construction)."""
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

    def test_a_spec_tag_stays_placeholdered_even_in_a_ref_based_group(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """``keep_ref=True`` only exempts the ``ref`` tag specifically —
        the header's own representative text still abstracts a separate
        ``[spec=...]`` tag (``ApmRepoError``'s other optional forensics
        field) away to a bare ``[spec]`` placeholder, since nothing
        establishes it's constant across the group the way ``ref`` itself
        is by construction; the real per-instance value still shows on
        each instance's own line regardless (same as any other
        instance-specific value, e.g. a digit or quoted path)."""
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
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        lines = result.output.replace("\n", "").split("  ")  # crude but enough to isolate the header line
        header = next(line for line in lines if line.startswith("[DataMissing]"))
        assert "[ref=shared-obj]" in header  # the ref tag: verbatim, shown once, in the header
        assert "[spec]" in header  # the spec tag: still placeholdered in the header, unlike ref
        assert "FORMAT-SPEC.md" not in header  # the header itself never leaks a concrete spec value
        # ... but each instance's own real spec value still appears on its own line, same as any
        # other instance-specific value (a digit, a quoted path) already does.
        assert "FORMAT-SPEC.md: A" in result.output
        assert "FORMAT-SPEC.md: B" in result.output

    def test_a_quoted_value_with_both_quote_characters_is_matched_as_one_span(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A raise site formatting a real path via ``{path!r}`` can embed
        Python's own backslash-escaped ``repr()`` output — e.g. a real
        filename containing both a ``'`` and a ``"`` forces ``repr()`` to
        keep ``'`` as the delimiter and escape the internal one
        (``repr("O'Brien's \\"backup\\".pst")`` is
        ``'O\\'Brien\\'s "backup".pst'``). A non-escape-aware quoted-span
        pattern stops at that first escaped ``'`` instead of the real
        closing one, leaking a mangled fragment (``Brien\\'``) straight
        into the group's own header text; ``_VARIABLE_RE`` must consume
        the whole span as one value instead, collapsing it to a single
        clean placeholder like every other quoted value."""
        path = repr("O'Brien's \"backup\".pst")
        findings = [
            Finding(
                Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", f"no file_map entry for path {path}"
            ),
        ]
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        # The real name legitimately (and correctly) still appears on the
        # instance's own bullet line -- only the *header*'s own normalized
        # template must never leak a mangled fragment of it.
        lines = result.output.replace("\n", "").split("  ")  # crude but enough to isolate the header line
        header = next(line for line in lines if line.startswith("[DataMissing]"))
        assert "Brien" not in header

    def test_findings_sharing_a_ref_but_different_templates_still_merge(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """``ref`` is the primary grouping key (per Part D.1) — two
        findings naming the same missing object merge even though their
        surrounding sentence differs, since it's the same underlying root
        cause either way. Mirrors the real
        ``snapshot_id``/``version_id`` shape ``cli_verify_sample1.json.gz``
        actually produces, where bare-digit differences ride alongside an
        identical ``ref``."""
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
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        unwrapped = result.output.replace("\n", "")
        assert unwrapped.count("no version_info row for") == 1
        assert unwrapped.count("(2 versions)") == 1
        # The digits still legitimately vary per instance even though ref
        # doesn't -- still shown, just no longer alongside a repeated ref.
        assert "2 1" in unwrapped
        assert "1 1" in unwrapped

    def test_findings_with_the_same_template_but_different_refs_do_not_merge(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The direct counterpart to the "different templates" test below,
        for the opposite axis: same sentence shape, but a different
        ``ref`` each -- two genuinely distinct missing objects, so they
        must render as two separate one-member groups, not merge into
        one."""
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
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        unwrapped = result.output.replace("\n", "")
        assert unwrapped.count("(1 version)") == 2  # two separate one-member groups, not one shared group
        assert "[ref=p1]" in unwrapped
        assert "[ref=p2]" in unwrapped

    def test_findings_with_no_ref_at_all_still_collapse_via_template_fallback(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Not every raise site attaches a ``ref`` (``units/saas/objectdb.py``'s
        own ``NotFoundError`` doesn't -- mirrored here) -- grouping must
        still fall back to the normalized-template match from Part D for
        these, exactly as before Part D.1."""
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
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        unwrapped = result.output.replace("\n", "")
        assert unwrapped.count("no object_table row for object_id=#") == 1
        assert unwrapped.count("(2 versions)") == 1  # count_label depends on Stage, not ref-vs-template
        assert "42" in unwrapped
        assert "43" in unwrapped

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

    def test_findings_with_different_templates_do_not_merge(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        findings = [
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:01", "no file_map entry for path"),
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-b/2026-01-01 00:00:02", "no such object"),
        ]
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert result.output.count("(1 version)") == 2  # two separate one-member groups, not merged
        assert "no file_map entry for path" in result.output
        assert "no such object" in result.output

    def test_findings_within_one_group_are_sorted_chronologically_regardless_of_discovery_order(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A group's own members still render chronologically, fed out
        of order -- _sort_key's own job, unaffected by this ask's change
        to *inter*-group order (see the next test)."""
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
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        unwrapped = result.output.replace("\n", "")
        assert "(3 versions)" in unwrapped
        assert (
            unwrapped.index("wl-a/2026-01-01") < unwrapped.index("wl-c/2026-02-01") < unwrapped.index("wl-b/2026-03-01")
        )

    def test_groups_are_sorted_by_their_own_ref_not_by_whichever_member_is_chronologically_first(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Two separate ref-groups order by their own ref value, not by
        which one's earliest member happens to sort chronologically
        first — the real shape that motivated this: two SaaS refs
        interleaving oddly in a report because one workload's display
        name happens to sort before the other's on the day both first
        appear. Ref "b"'s only member (2026-01-01) is chronologically
        earlier than ref "a"'s (2026-01-02) — first-member ordering
        would put "b" first; ref-based ordering must still put "a"
        first."""
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
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert result.output.index("[ref=a]") < result.output.index("[ref=b]")

    def test_a_non_per_version_stage_version_finding_sorts_by_whole_path_not_a_bogus_timestamp(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Not every ``Stage.VERSION`` finding is per-version — a
        connection-enumeration failure (``units/verify_reachable.py``'s
        ``_unresolvable_finding(repo.layout.repo_root, exc)``) uses a bare
        filesystem path, with no ``"<workload>/<version>"`` shape and no
        trailing timestamp at all. These two findings share the same
        stage+symptom, so the sort depends entirely on ``_sort_key``'s
        tiebreak, and the two candidate strategies disagree on the
        result: naively trusting the last ``/``-separated segment
        ("root") sorts *after* a real timestamp (`"r" > "2"`), while
        falling back to the whole path ("/repo/root") sorts *before* one
        (`"/" < "2"`) — this pins down the latter, correct behavior."""
        findings = [
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "wl-a/2026-01-01 00:00:00", "reason A"),
            Finding(Stage.VERSION, Symptom.DATA_MISSING, "/repo/root", "connection enumeration failed"),
        ]
        _patch_verify_findings(monkeypatch, findings)
        result = runner.invoke(app, ["verify", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert result.output.index("connection enumeration failed") < result.output.index("reason A")

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
    """``Symptom.REPAIRED_VIA_PARITY`` findings are a successful self-heal,
    not a problem left unresolved (see that symptom's own docstring) --
    ``_render_human`` must not fold them into the red "problem" count."""

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
