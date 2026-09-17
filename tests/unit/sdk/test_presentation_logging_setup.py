"""Unit tests for ``synology_apm_repo.sdk.presentation.logging_setup`` --
the mechanism shared by the CLI's and TUI's own entry points (``cli/main.py``
and ``browser/app.py``). Each of those only calls ``configure_logging()``
and is covered separately for that call itself
(``test_cli_main.py``'s/``test_browser_app.py``'s own ordering tests);
the actual behaviour is proven once, here."""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

import pytest

from synology_apm_repo.sdk.presentation import logging_setup

#: Driven in a child process on purpose: pytest's own logging plugin
#: installs handlers on the root logger for the duration of every test
#: item, and ``Logger.callHandlers`` only falls back to the last-resort
#: handler when it finds *no* handler at all. In-process, stderr therefore
#: stays clean whether or not ``configure_logging()`` ran, so an in-process
#: version of this test would pass with the fix deleted.
_CHILD_PROGRAM = """
import logging
import sys

if sys.argv[1] == "configured":
    from synology_apm_repo.sdk.presentation.logging_setup import configure_logging

    configure_logging()

logging.getLogger("somedependency.transport").warning("noise a user cannot act on")
"""


@pytest.mark.parametrize(("mode", "reaches_stderr"), [("bare", True), ("configured", False)])
def test_configure_logging_is_what_keeps_stderr_clean(
    monkeypatch: pytest.MonkeyPatch, mode: str, reaches_stderr: bool
) -> None:
    """Both halves in one test: the ``bare`` case is the behaviour being
    fixed, the ``configured`` case is the fix, and running them the same way
    is what makes the second one mean anything."""
    monkeypatch.delenv(logging_setup.LOG_FILE_ENV, raising=False)
    result = subprocess.run(
        [sys.executable, "-c", _CHILD_PROGRAM, mode],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert ("noise a user cannot act on" in result.stderr) is reaches_stderr


def test_the_env_var_routes_it_to_a_file_instead(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The escape hatch: nothing reaches the terminal either way, but a
    backend can still be debugged — including one whose share or path names
    are not ASCII, which the machine's locale encoding would mangle."""
    message = "noise about the share unicode-shäre-ø"
    log_file = tmp_path / "apm.log"
    monkeypatch.setenv(logging_setup.LOG_FILE_ENV, str(log_file))
    root = logging.getLogger()
    before, before_level = list(root.handlers), root.level
    added: list[logging.Handler] = []
    try:
        root.handlers[:] = []
        logging_setup.configure_logging()
        added = list(root.handlers)
        logging.getLogger("somedependency.transport").warning(message)
        for handler in added:
            handler.flush()
    finally:
        root.handlers[:] = before
        root.setLevel(before_level)
        # An open FileHandler keeps a Windows lock on a file under tmp_path.
        for handler in added:
            handler.close()
    # Asserted directly rather than only via the round trip below, which
    # proves nothing on a machine whose locale encoding is already UTF-8.
    assert [getattr(handler, "encoding", None) for handler in added] == ["utf-8"]
    assert message in log_file.read_text(encoding="utf-8")
