"""Unit tests for ``synology_apm_repo.cli.paging``. ``paged()``'s
subprocess-invoking branch is deliberately never exercised here — a real
``less`` process reading real stdin isn't something a unit test should
shell out to — see each test's own docstring for what it checks instead.
"""

from __future__ import annotations

import subprocess
from io import StringIO

import pytest
from rich.console import Console

from synology_apm_repo.cli.paging import _SubprocessPager, paged, pager_argv


class TestPagerArgv:
    def test_defaults_to_less_firx_when_pager_is_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("PAGER", raising=False)
        assert pager_argv() == ["less", "-FIRX"]

    def test_honors_a_custom_pager_with_its_own_arguments(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PAGER", "most -s")
        assert pager_argv() == ["most", "-s"]

    def test_empty_pager_means_no_paging_at_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PAGER", "")
        assert pager_argv() is None


class TestPaged:
    def test_is_a_no_op_when_stdout_is_not_a_terminal(self) -> None:
        # Console(file=StringIO()) is never a terminal -- the shape every
        # CliRunner-driven command test already runs under.
        buffer = StringIO()
        console = Console(file=buffer)
        with paged(console):
            console.print("hello")
        assert "hello" in buffer.getvalue()  # printed immediately, no pager involved

    def test_is_a_no_op_when_pager_is_explicitly_empty_even_on_a_real_terminal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PAGER", "")
        buffer = StringIO()
        console = Console(file=buffer, force_terminal=True)
        with paged(console):
            console.print("hello")
        assert "hello" in buffer.getvalue()

    def test_actually_pages_on_a_real_terminal_with_a_resolved_pager(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The one branch neither test above reaches: is_terminal True *and*
        # a pager resolved -- console.pager()'s own PagerContext.__exit__
        # calls Pager.show(), which we intercept here rather than shelling
        # out to a real interactive pager from a unit test.
        shown: list[str] = []
        monkeypatch.setattr(_SubprocessPager, "show", lambda self, content: shown.append(content))
        console = Console(file=StringIO(), force_terminal=True)
        with paged(console):
            console.print("paged content")
        assert shown and "paged content" in shown[0]


class TestSubprocessPager:
    def test_shows_content_via_the_configured_pager(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[tuple[list[str], str]] = []

        def _fake_run(argv: list[str], *, input: str, text: bool, check: bool) -> object:
            calls.append((argv, input))
            return None

        monkeypatch.setattr(subprocess, "run", _fake_run)
        _SubprocessPager(["less", "-FIRX"]).show("some content")
        assert calls == [(["less", "-FIRX"], "some content")]

    def test_falls_back_to_printing_directly_when_the_pager_binary_is_missing(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # A pager that can't possibly exist on $PATH -- proves a missing
        # pager never crashes the command it's wrapping.
        _SubprocessPager(["definitely-not-a-real-pager-binary"]).show("fallback content")
        assert capsys.readouterr().out == "fallback content"


__all__: list[str] = []
