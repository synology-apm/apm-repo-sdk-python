"""Unit test for ``synology_apm_repo.cli.main``'s root callback
``--progress`` validation — every other CLI test passes a valid value (or
omits the flag), so Typer/Click's own enum-choice rejection is otherwise
never exercised here."""

from __future__ import annotations

from importlib.metadata import version as pkg_version

import pytest
from typer.testing import CliRunner

from synology_apm_repo.cli import main as main_module
from synology_apm_repo.cli.main import app

runner = CliRunner()


def test_invalid_progress_value_is_rejected() -> None:
    result = runner.invoke(app, ["--progress", "sometimes", "doctor"])
    assert result.exit_code != 0
    # Click's own enum-choice message, asserted piecewise since the error
    # box wraps long lines (splitting the message text itself).
    assert "Invalid value for '--progress'" in result.output
    assert "'sometimes'" in result.output
    assert "is not one of" in result.output


@pytest.mark.parametrize("value", ["auto", "always", "never"])
def test_valid_progress_values_reach_the_subcommand(value: str) -> None:
    # A valid --progress must never itself be rejected — the root
    # callback runs to completion and ``doctor`` fails for its own,
    # unrelated reason (no REPO/--profile given), proving _root() didn't
    # raise.
    result = runner.invoke(app, ["--progress", value, "doctor"])
    assert result.exit_code == 1
    assert "exactly one of REPO or --profile" in result.output


def test_dash_h_is_an_alias_for_help_at_the_root() -> None:
    # Click's own default help_option_names is ["--help"] only — main.py's
    # Typer() sets context_settings={"help_option_names": ["-h", "--help"]}
    # to accept "-h" as the other half of that pair.
    result = runner.invoke(app, ["-h"])
    assert result.exit_code == 0
    assert result.output == runner.invoke(app, ["--help"]).output


def test_dash_h_is_an_alias_for_help_on_a_subcommand() -> None:
    # context_settings on the root Typer() must inherit into every
    # subcommand's own Click context, not just the root's.
    result = runner.invoke(app, ["doctor", "-h"])
    assert result.exit_code == 0
    assert result.output == runner.invoke(app, ["doctor", "--help"]).output


def test_version_prints_the_installed_distribution_version_and_exits() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert result.output.strip() == f"synology-apm-repo-cli {pkg_version('synology-apm-repo-cli')}"


def test_version_is_eager_and_short_circuits_before_any_subcommand_is_needed() -> None:
    # --version alone (no COMMAND) must not fall through to
    # no_args_is_help's "show help" behavior instead.
    result = runner.invoke(app, ["--version"])
    assert "Usage:" not in result.output


__all__: list[str] = []


def test_main_configures_logging_before_running_the_app(monkeypatch: pytest.MonkeyPatch) -> None:
    # main() itself is excluded from the coverage gate (real sys.argv/
    # sys.exit) -- this only proves the ordering that matters:
    # configure_logging() must run before app() does anything. The
    # mechanism itself (shared with the TUI's own call site) is proven in
    # test_presentation_logging_setup.py, once, not here.
    calls: list[str] = []
    monkeypatch.setattr(main_module, "configure_logging", lambda: calls.append("configure_logging"))
    monkeypatch.setattr(main_module, "app", lambda: calls.append("app"))

    main_module.main()

    assert calls == ["configure_logging", "app"]
