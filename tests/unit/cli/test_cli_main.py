"""Unit tests for ``synology_apm_repo.cli.main``'s root callback: ``--progress``
validation, ``-h``, ``--version``, and ``main()``'s logging setup."""

from __future__ import annotations

from importlib.metadata import version as pkg_version

import pytest

from support.cli import invoke
from synology_apm_repo.cli import main as main_module


def test_invalid_progress_value_is_rejected() -> None:
    result = invoke(["--progress", "sometimes", "doctor"], exit_code=None)
    assert result.exit_code != 0
    # Click's own enum-choice message, asserted piecewise since the error
    # box wraps long lines (splitting the message text itself).
    assert "Invalid value for '--progress'" in result.output
    assert "'sometimes'" in result.output
    assert "is not one of" in result.output


@pytest.mark.parametrize("value", ["auto", "always", "never"])
def test_valid_progress_values_reach_the_subcommand(value: str) -> None:
    # ``doctor`` failing for its own reason (no REPO/--profile) proves the
    # root callback accepted the value.
    result = invoke(["--progress", value, "doctor"], exit_code=1)
    assert "exactly one of REPO or --profile" in result.output


def test_dash_h_is_an_alias_for_help_at_the_root() -> None:
    result = invoke(["-h"])
    assert result.output == invoke(["--help"], exit_code=None).output


def test_dash_h_is_an_alias_for_help_on_a_subcommand() -> None:
    # context_settings on the root Typer() must inherit into every
    # subcommand's own Click context, not just the root's.
    result = invoke(["doctor", "-h"])
    assert result.output == invoke(["doctor", "--help"], exit_code=None).output


def test_version_prints_the_installed_distribution_version_and_exits() -> None:
    result = invoke(["--version"])
    assert result.output.strip() == f"synology-apm-repo-cli {pkg_version('synology-apm-repo-cli')}"


def test_version_is_eager_and_short_circuits_before_any_subcommand_is_needed() -> None:
    # --version alone (no COMMAND) must not fall through to
    # no_args_is_help's "show help" behavior instead.
    result = invoke(["--version"], exit_code=None)
    assert "Usage:" not in result.output


def test_main_configures_logging_before_running_the_app(monkeypatch: pytest.MonkeyPatch) -> None:
    # configure_logging() itself is covered by
    # tests/unit/sdk/test_presentation_logging_setup.py.
    calls: list[str] = []
    monkeypatch.setattr(main_module, "configure_logging", lambda: calls.append("configure_logging"))
    monkeypatch.setattr(main_module, "app", lambda: calls.append("app"))

    main_module.main()

    assert calls == ["configure_logging", "app"]
