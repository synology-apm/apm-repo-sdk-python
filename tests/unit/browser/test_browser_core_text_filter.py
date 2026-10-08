"""Unit tests for ``synology_apm_repo.browser.core.text_filter``."""

from __future__ import annotations

from synology_apm_repo.browser.core.text_filter import matches_filter


def test_an_empty_needle_keeps_everything() -> None:
    assert matches_filter("", "anything")
    assert matches_filter("", "")


def test_a_needle_matches_a_substring_ignoring_case_on_both_sides() -> None:
    assert matches_filter("INBOX", "My inbox (2)")
    assert matches_filter("inbox", "MY INBOX")


def test_a_needle_absent_from_the_text_filters_it_out() -> None:
    assert not matches_filter("sent", "Inbox")
