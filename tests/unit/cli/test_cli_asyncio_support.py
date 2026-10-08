"""Unit tests for ``synology_apm_repo.cli.asyncio_support``."""

from __future__ import annotations

import inspect

import pytest
import typer

from synology_apm_repo.cli.asyncio_support import typer_async


def test_wrapped_function_runs_to_completion_and_returns_its_result() -> None:
    @typer_async
    async def add_one(x: int) -> int:
        return x + 1

    assert add_one(41) == 42


def test_wrapped_function_forwards_args_and_kwargs() -> None:
    @typer_async
    async def combine(a: int, *, b: str) -> str:
        return f"{a}-{b}"

    assert combine(1, b="two") == "1-two"


def test_wrapped_function_turns_an_unexpected_exception_into_a_clean_exit(capsys: pytest.CaptureFixture[str]) -> None:
    # typer_async is the one chokepoint every command passes through, so it
    # is where fail_unexpected() turns a bug into a reported exit 1.
    @typer_async
    async def raises() -> None:
        raise ValueError("boom")

    with pytest.raises(typer.Exit) as exc_info:
        raises()
    assert exc_info.value.exit_code == 1
    stderr = capsys.readouterr().err
    assert "internal error: ValueError: boom" in stderr
    assert "Traceback" in stderr
    assert "please file it at" in stderr


def test_wrapped_function_lets_a_typer_exit_pass_through_unchanged() -> None:
    # A command's own fail() raises typer.Exit; it must not be rewrapped as
    # an "internal error".
    @typer_async
    async def exits() -> None:
        raise typer.Exit(code=3)

    with pytest.raises(typer.Exit) as exc_info:
        exits()
    assert exc_info.value.exit_code == 3


def test_an_unhandled_ctrl_c_exits_cancelled(capsys: pytest.CaptureFixture[str]) -> None:
    # asyncio.run surfaces a SIGINT as KeyboardInterrupt; any command that
    # doesn't handle its own cancellation must still exit 130, not Click's
    # generic "Aborted!" with status 1.
    @typer_async
    async def interrupted() -> None:
        raise KeyboardInterrupt

    with pytest.raises(typer.Exit) as exc_info:
        interrupted()
    assert exc_info.value.exit_code == 130
    assert "cancelled" in capsys.readouterr().err


def test_signature_is_preserved_for_typer_introspection() -> None:
    # Typer builds a command's options/arguments from this signature.
    async def original(x: int, *, y: str = "default") -> None:
        pass

    wrapped = typer_async(original)
    assert inspect.signature(wrapped) == inspect.signature(original)
