"""Unit tests for ``synology_apm_repo.cli.errors``'s ``friendly_message()``
and ``fail_from_apm_error()`` — synthetic ``ApmRepoError``/
``KeyRequiredError``/``KeyMismatchError`` instances, no real repository
needed."""

from __future__ import annotations

import io
from contextlib import redirect_stderr

import pytest
import typer

from synology_apm_repo.cli.errors import fail_from_apm_error, friendly_message
from synology_apm_repo.cli.state import CliState, ProgressMode
from synology_apm_repo.sdk.errors import ApmRepoError, KeyMismatchError, KeyRequiredError, NotFoundError


def test_key_required_with_no_ref_is_rephrased_for_the_cli() -> None:
    # The exact shape Repository._require_key_verified() raises: no ref,
    # since it's a whole-repository gate with no single file to point at.
    exc = KeyRequiredError("this repository is encrypted; call set_key() before browsing workloads/versions")
    message = friendly_message(exc)
    assert "this repository is encrypted" in message
    assert "--key" in message
    assert "synology-apm-repo-cli key" in message
    assert "set_key" not in message


def test_key_mismatch_with_no_ref_is_rephrased_for_the_cli() -> None:
    exc = KeyMismatchError(
        "the key previously supplied for this repository was rejected; "
        "call set_key() with a valid key before browsing workloads/versions"
    )
    message = friendly_message(exc)
    assert "the key given was rejected" in message
    assert "--key" in message
    assert "set_key" not in message


def test_key_required_with_a_ref_falls_through_to_the_general_case() -> None:
    # pool.py/sqlite_source.py raise the same two types for one specific
    # file (a ref set) -- not the whole-repository gate's special rephrasing,
    # so this is just an ordinary ApmRepoError as far as friendly_message
    # is concerned: safe_message by default, ref restored under verbose.
    exc = KeyRequiredError(
        "data is aHlT-enveloped but no vault_key was given", ref="@ActiveProtectVault/@data/Pool/45/0"
    )
    assert friendly_message(exc) == exc.safe_message
    assert friendly_message(exc, verbose=True) == str(exc)


def test_key_mismatch_with_a_ref_falls_through_to_the_general_case() -> None:
    exc = KeyMismatchError("AES-256-GCM tag check failed", ref="@ActiveProtectVault/@data/Pool/0/0.buk")
    assert friendly_message(exc) == exc.safe_message
    assert friendly_message(exc, verbose=True) == str(exc)


def test_an_unrelated_apm_repo_error_strips_ref_by_default_and_restores_it_verbose() -> None:
    # The message text itself may still name the same path in plain
    # English (this raise site does, redundantly, with its own ref=) --
    # safe_message only strips the structured "[ref=...]" tag appended
    # by ApmRepoError.__str__, not every occurrence of the value.
    exc = NotFoundError("something went wrong reading it", ref="/tmp/x")
    assert friendly_message(exc) == exc.safe_message
    assert "[ref=" not in friendly_message(exc)
    message = friendly_message(exc, verbose=True)
    assert message == str(exc)
    assert "[ref=/tmp/x]" in message


def test_returned_message_is_a_plain_str_not_the_exception_itself() -> None:
    exc: ApmRepoError = KeyRequiredError("this repository is encrypted; call set_key() before browsing")
    assert isinstance(friendly_message(exc), str)


# -- fail_from_apm_error() -- the shared tail every open/store skeleton uses --


def test_fail_from_apm_error_raises_typer_exit_code_1() -> None:
    exc = NotFoundError("nothing readable at this location", ref="/some/internal/path")
    buf = io.StringIO()
    with redirect_stderr(buf), pytest.raises(typer.Exit) as excinfo:
        fail_from_apm_error(exc, CliState())
    assert excinfo.value.exit_code == 1
    assert excinfo.value.__cause__ is exc


def test_fail_from_apm_error_respects_the_same_verbose_gate_as_friendly_message() -> None:
    exc = NotFoundError("nothing readable at this location", ref="/some/internal/path")

    buf = io.StringIO()
    with redirect_stderr(buf), pytest.raises(typer.Exit):
        fail_from_apm_error(exc, CliState(verbose=False))
    assert "/some/internal/path" not in buf.getvalue()

    buf = io.StringIO()
    with redirect_stderr(buf), pytest.raises(typer.Exit):
        fail_from_apm_error(exc, CliState(verbose=True))
    assert "/some/internal/path" in buf.getvalue()


def test_fail_from_apm_error_applies_its_own_prefix() -> None:
    exc = ApmRepoError("connection refused")
    buf = io.StringIO()
    with redirect_stderr(buf), pytest.raises(typer.Exit):
        fail_from_apm_error(exc, CliState(), prefix="connectivity check failed: ")
    assert "connectivity check failed: connection refused" in buf.getvalue()


def test_fail_from_apm_error_clears_a_live_progress_line_first() -> None:
    """The clear must land *before* ``fail()``'s own error print — a
    ``finally``-only clear would run after that print, too late to stop
    the error message from landing on top of a dangling progress line."""
    exc = ApmRepoError("boom")
    buf = io.StringIO()
    with redirect_stderr(buf), pytest.raises(typer.Exit):
        fail_from_apm_error(exc, CliState(progress=ProgressMode.ALWAYS))
    output = buf.getvalue()
    assert "\x1b[2K" in output
    assert output.index("\x1b[2K") < output.index("error:")


__all__: list[str] = []
