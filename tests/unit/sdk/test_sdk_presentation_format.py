"""Unit tests for ``synology_apm_repo.sdk.presentation.format`` — the
size/duration/count formatters shared by the CLI and the TUI (displayed
numbers must agree across both, so the formatting logic lives here once,
not twice)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from synology_apm_repo.sdk.presentation.format import (
    format_bytes,
    format_duration,
    format_rate,
    format_timestamp,
    pluralize,
)


def test_singular_count_returns_the_singular_form() -> None:
    assert pluralize(1, "item") == "item"


def test_zero_count_returns_the_default_plural_form() -> None:
    assert pluralize(0, "item") == "items"


def test_plural_count_returns_the_default_plural_form() -> None:
    assert pluralize(3, "item") == "items"


def test_plural_count_with_an_explicit_irregular_plural() -> None:
    assert pluralize(3, "repository", "repositories") == "repositories"


def test_singular_count_ignores_an_explicit_plural_argument() -> None:
    assert pluralize(1, "repository", "repositories") == "repository"


@pytest.mark.parametrize(
    ("n", "expected"),
    [
        (0, "0 B"),
        (512, "512 B"),
        (1024, "1.0 KiB"),
        (1536, "1.5 KiB"),
        (1024 * 1024, "1.0 MiB"),
        (1024**3, "1.0 GiB"),
        (1024**4, "1.0 TiB"),
        (int(1.5 * 1024**4), "1.5 TiB"),
    ],
)
def test_format_bytes(n: int, expected: str) -> None:
    assert format_bytes(n) == expected


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "00:00"),
        (5, "00:05"),
        (65, "01:05"),
        (3600, "01:00:00"),
        (3725, "01:02:05"),
    ],
)
def test_format_duration(seconds: float, expected: str) -> None:
    assert format_duration(seconds) == expected


@pytest.mark.parametrize(
    ("rate", "unit", "expected"),
    [
        (1024 * 1024, "bytes", "1.0 MiB/s"),
        (512, "bytes", "512 B/s"),
        (4.0, "files", "4.0 files/s"),
        (12.34, "objects", "12.3 objects/s"),
    ],
)
def test_format_rate(rate: float, unit: str, expected: str) -> None:
    assert format_rate(rate, unit) == expected


def test_format_timestamp_renders_in_the_pinned_local_timezone() -> None:
    # tests/conftest.py's session-scoped _fixed_timezone fixture pins the
    # process to Asia/Taipei (+08:00, no DST observed since 1979) — same
    # fixture catalog/version.py's _version_display_name relies on for its
    # own local-time rendering.
    dt = datetime.fromtimestamp(0, UTC)
    assert format_timestamp(dt) == "1970-01-01 08:00:00"


__all__: list[str] = []
