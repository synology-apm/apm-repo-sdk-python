"""``api/export_paths.py``: export file-name safety."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

from synology_apm_repo.sdk.api.export_paths import safe_export_join, safe_file_name, windows_reserved_basename

# -- windows_reserved_basename() ------------------------------------------


@pytest.mark.parametrize("name", ["CON", "con", "Nul", "AUX.txt", "prn.tar.gz", "COM1", "com9.log", "LPT1", "lpt9"])
def test_windows_reserved_basename_matches_device_names_with_or_without_an_extension(name: str) -> None:
    assert windows_reserved_basename(name)


@pytest.mark.parametrize("name", ["console", "COM0", "COM10", "LPT0", "nulls", "my.con", "", "CONX.txt"])
def test_windows_reserved_basename_leaves_other_names_alone(name: str) -> None:
    assert not windows_reserved_basename(name)


# -- safe_export_join() ---------------------------------------------------
#
# Node.name/RestorableUnit.name are display names, not guaranteed to be one
# safe path component (a name can come from a file inside a backed-up guest).


class TestSafeExportJoin:
    def test_joins_one_safe_segment(self, tmp_path: Path) -> None:
        assert safe_export_join(tmp_path, "report.pdf") == tmp_path / "report.pdf"

    def test_joins_several_segments_one_level_at_a_time(self, tmp_path: Path) -> None:
        # The shape of a recursive tree walk: one more segment per call.
        level1 = safe_export_join(tmp_path, "Documents")
        level2 = safe_export_join(level1, "2024")
        level3 = safe_export_join(level2, "report.pdf")
        assert level3 == tmp_path / "Documents" / "2024" / "report.pdf"

    def test_joins_a_whole_chain_in_one_call(self, tmp_path: Path) -> None:
        expected = tmp_path / "Documents" / "2024" / "report.pdf"
        assert safe_export_join(tmp_path, "Documents", "2024", "report.pdf") == expected

    def test_no_segments_returns_root_unchanged(self, tmp_path: Path) -> None:
        assert safe_export_join(tmp_path) == tmp_path

    @pytest.mark.parametrize(
        "segment",
        [
            "",
            ".",
            "..",
            "../secret",
            "a/../../secret",
            "a/b",
            "/etc/passwd",
            "a\\b",
            "..\\secret",
            "C:\\Windows",
            "\\\\server\\share",
            "a\x00b",
        ],
    )
    def test_rejects_an_unsafe_segment(self, tmp_path: Path, segment: str) -> None:
        with pytest.raises(ValueError, match=re.escape(f"unsafe export path segment: {segment!r}")):
            safe_export_join(tmp_path, segment)

    @pytest.mark.parametrize(
        "segment", ["C:foo", "con", "NUL.txt", "a:b", 'a"b', "a|b", "a?b", "a*b", "name.", "name "]
    )
    def test_those_are_ordinary_file_names_off_windows(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, segment: str
    ) -> None:
        monkeypatch.setattr(sys, "platform", "linux")
        assert safe_export_join(tmp_path, segment) == tmp_path / segment

    @pytest.mark.parametrize(
        "segment",
        [
            "C:foo",  # drive-relative: Path(root) / "C:foo" would leave root on Windows
            "C:",
            "a:b",  # an alternate data stream
            "con",
            "NUL",
            "com1.txt",
            "LPT9.tar.gz",
            'a"b',
            "a<b",
            "a>b",
            "a|b",
            "a?b",
            "a*b",
            "a\x01b",
            "name.",
            "name ",
        ],
    )
    def test_rejects_what_windows_does_not_treat_as_a_plain_file_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, segment: str
    ) -> None:
        monkeypatch.setattr(sys, "platform", "win32")

        with pytest.raises(ValueError, match=re.escape(f"unsafe export path segment: {segment!r}")):
            safe_export_join(tmp_path, segment)

    @pytest.mark.parametrize(
        "segment", ["report.pdf", "résumé (final) v2.docx", "console.log", "COM10", "a.b.c", ".hidden"]
    )
    def test_still_joins_ordinary_names_on_windows(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, segment: str
    ) -> None:
        monkeypatch.setattr(sys, "platform", "win32")

        assert safe_export_join(tmp_path, segment) == tmp_path / segment

    def test_rejects_an_unsafe_segment_anywhere_in_a_multi_segment_call_not_just_the_last(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match=re.escape("unsafe export path segment: '..'")):
            safe_export_join(tmp_path, "Documents", "..", "report.pdf")

    def test_a_legitimate_name_containing_special_but_safe_characters_still_joins(self, tmp_path: Path) -> None:
        name = "résumé (final) v2.docx"
        assert safe_export_join(tmp_path, name) == tmp_path / name

    def test_root_itself_is_never_validated(self, tmp_path: Path) -> None:
        # root is the caller's trusted directory; only segments are validated.
        weird_root = tmp_path / ".."
        assert safe_export_join(weird_root, "file.txt") == weird_root / "file.txt"


# -- safe_file_name() -------------------------------------------------------


class TestSafeFileName:
    """Every result must pass ``safe_export_join`` on the platform it was made for."""

    @pytest.mark.parametrize(
        ("name", "expected"),
        [("Re: hi.eml", "Re: hi.eml"), ("a/b\\c.eml", "a_b_c.eml"), ("a\x00b", "a_b"), ("", "_"), ("..", "_")],
    )
    def test_off_windows_only_separators_and_nul_are_replaced(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, expected: str
    ) -> None:
        monkeypatch.setattr(sys, "platform", "linux")

        assert safe_file_name(name) == expected
        assert safe_export_join(tmp_path, expected) == tmp_path / expected

    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("Re: hi.eml", "Re_ hi.eml"),
            ('a<b>c"d|e?f*g.ics', "a_b_c_d_e_f_g.ics"),
            ("trailing. ", "trailing"),
            ("CON.eml", "_CON.eml"),
            ("...", "_"),
        ],
    )
    def test_on_windows_every_forbidden_character_and_reserved_name_is_made_safe(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, expected: str
    ) -> None:
        monkeypatch.setattr(sys, "platform", "win32")

        assert safe_file_name(name) == expected
        assert safe_export_join(tmp_path, expected) == tmp_path / expected
