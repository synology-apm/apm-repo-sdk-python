"""Unit tests for ``synology_apm_repo.sdk.presentation.logging_setup``,
shared by the CLI's and TUI's entry points (``cli/main.py``,
``browser/app.py``), whose own tests only check that they call
``configure_logging()``."""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

import pytest

from synology_apm_repo.sdk.presentation import logging_setup

#: Run in a child process: pytest's logging plugin keeps a handler on the
#: root logger, so in-process the last-resort handler never writes to stderr
#: and the test could not fail.
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
    """The ``bare`` run is the control: without ``configure_logging()`` the
    warning does reach stderr."""
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
    """The log file is written as UTF-8, so non-ASCII share or path names
    survive whatever the locale encoding is."""
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
