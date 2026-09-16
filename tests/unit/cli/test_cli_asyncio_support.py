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
    # Intentional behavior change: a bug the wrapped call doesn't resolve
    # into a typer.Exit itself no longer propagates as a raw exception —
    # fail_unexpected() turns it into a typer.Exit(code=1), the same clean
    # failure shape every *expected* command error already produces via
    # fail()/unwrap(). See errors.py::fail_unexpected's own docstring.
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
    # A command's own fail()/unwrap() already raises typer.Exit — that
    # must reach the caller exactly as raised, never get rewrapped as an
    # "internal error".
    @typer_async
    async def exits() -> None:
        raise typer.Exit(code=3)

    with pytest.raises(typer.Exit) as exc_info:
        exits()
    assert exc_info.value.exit_code == 3


def test_signature_is_preserved_for_typer_introspection() -> None:
    # Typer inspects a command's signature to build --options/arguments —
    # functools.wraps must keep the original async function's signature
    # visible through the sync wrapper, not the wrapper's own (*args,
    # **kwargs).
    async def original(x: int, *, y: str = "default") -> None:
        pass

    wrapped = typer_async(original)
    assert inspect.signature(wrapped) == inspect.signature(original)
