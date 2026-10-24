"""Unit tests for ``synology_apm_repo.cli.consoles``."""

from __future__ import annotations

import io
import sys

import pytest
from rich.console import Console

from synology_apm_repo.cli.consoles import escape_unencodable_output


def _patch_cp950_stdio(monkeypatch: pytest.MonkeyPatch) -> tuple[io.BytesIO, io.BytesIO]:
    """Replaces both streams, so the call never reconfigures pytest's own
    capture; returns their (stdout, stderr) bytes."""
    raws = (io.BytesIO(), io.BytesIO())
    for name, raw in zip(("stdout", "stderr"), raws, strict=True):
        stream = io.TextIOWrapper(raw, encoding="cp950", errors="strict", newline="\n", write_through=True)
        monkeypatch.setattr(sys, name, stream)
    return raws


class TestEscapeUnencodableOutput:
    def test_a_cp950_stdout_escapes_an_emoji_and_keeps_its_encoding_for_cjk(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        out, _ = _patch_cp950_stdio(monkeypatch)
        escape_unencodable_output()
        Console(file=sys.stdout, width=80).print("名稱 \U0001f60b")
        assert out.getvalue().decode("cp950") == "名稱 \\U0001f60b\n"

    def test_stderr_is_reconfigured_too(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, err = _patch_cp950_stdio(monkeypatch)
        escape_unencodable_output()
        sys.stderr.write("\U0001f60b")
        assert err.getvalue() == b"\\U0001f60b"

    def test_a_stream_that_is_not_a_text_wrapper_is_left_alone(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # e.g. pythonw.exe's None, or a test runner's StringIO capture.
        monkeypatch.setattr(sys, "stdout", None)
        monkeypatch.setattr(sys, "stderr", io.StringIO())
        escape_unencodable_output()  # must not raise
