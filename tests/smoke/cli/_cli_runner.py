"""Subprocess wrapper around the real, installed ``synology-apm-repo-cli``
console script -- the one thing this smoke tool exists to exercise that
neither ``tests/unit/cli/`` nor ``tests/integration/cli/`` do (both drive
``synology_apm_repo.cli.main.app`` in-process via ``typer.testing.
CliRunner``, never the packaged entry point as a real subprocess).
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

#: Default wall-clock budget for one invocation -- generous enough for a
#: real (small) export/verify against a real sample, but still bounded so
#: a genuinely hung subprocess doesn't stall the whole tool.
_DEFAULT_TIMEOUT = 120


def _decode(raw: bytes) -> str:
    """Lossy decode for report/log display only -- ``cat``'s stdout can be
    arbitrary binary content (a VM disk image chunk, ...), never
    necessarily valid UTF-8, so this never raises the way
    ``subprocess.run(text=True)``'s default strict decoding would."""
    return raw.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class CliResult:
    args: list[str]
    exit_code: int
    stdout: str
    stderr: str
    stdout_bytes: int
    timed_out: bool = False


class CliRunner:
    """Invokes the real ``synology-apm-repo-cli`` binary via ``uv run``,
    with two defaults baked into every call (see this module's own
    docstring for why):

    - ``--no-input`` is always passed, so a ``profile add``/``remove``
      invocation never blocks forever on an interactive prompt with no
      attached tty.
    - ``PAGER=""`` is always set, so ``tree``/``dump`` never pipe their
      output through a real pager regardless of tty-detection nuances --
      an explicitly empty ``$PAGER`` is this CLI's own documented
      "never page" signal (``cli/paging.py``).

    Captures raw bytes throughout (never ``subprocess.run(text=True)``,
    whose default strict decoding would crash on ``cat``'s binary
    content) -- ``CliResult.stdout``/``.stderr`` are lossily decoded for
    report display only; ``.stdout_bytes`` is the real byte count for any
    check that cares about actual size.
    """

    def run(
        self,
        *args: str,
        env_overrides: dict[str, str] | None = None,
        timeout: float = _DEFAULT_TIMEOUT,
        input_text: str | None = None,
    ) -> CliResult:
        argv = ["uv", "run", "synology-apm-repo-cli", "--no-input", *args]
        env = {**os.environ, "PAGER": "", **(env_overrides or {})}
        input_bytes = input_text.encode("utf-8") if input_text is not None else None
        try:
            proc = subprocess.run(argv, capture_output=True, env=env, timeout=timeout, input=input_bytes)
        except subprocess.TimeoutExpired as exc:
            stdout_raw = exc.stdout or b""
            return CliResult(
                list(args),
                exit_code=-1,
                stdout=_decode(stdout_raw),
                stderr=_decode(exc.stderr or b""),
                stdout_bytes=len(stdout_raw),
                timed_out=True,
            )
        return CliResult(
            list(args),
            exit_code=proc.returncode,
            stdout=_decode(proc.stdout),
            stderr=_decode(proc.stderr),
            stdout_bytes=len(proc.stdout),
        )

    def run_cancellable(
        self,
        *args: str,
        env_overrides: dict[str, str] | None = None,
        cancel_after: float,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> CliResult:
        """Start the real subprocess, wait ``cancel_after`` seconds, then
        send a real ``SIGINT`` -- the one check that's genuinely
        subprocess-only (``export``'s real Ctrl-C double-press
        cancellation), impossible to exercise through an in-process
        ``CliRunner``."""
        argv = ["uv", "run", "synology-apm-repo-cli", "--no-input", *args]
        env = {**os.environ, "PAGER": "", **(env_overrides or {})}
        proc: subprocess.Popen[bytes] = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        time.sleep(cancel_after)
        proc.send_signal(signal.SIGINT)
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
            return CliResult(
                list(args),
                exit_code=proc.returncode,
                stdout=_decode(stdout),
                stderr=_decode(stderr),
                stdout_bytes=len(stdout),
            )
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            return CliResult(
                list(args),
                exit_code=-1,
                stdout=_decode(stdout),
                stderr=_decode(stderr),
                stdout_bytes=len(stdout),
                timed_out=True,
            )

    def sandboxed_env(self, home_dir: Path) -> dict[str, str]:
        """``env_overrides`` for a ``profile add/list/show/remove`` round
        trip -- points every config-file lookup at a per-run temp
        directory instead of the developer's real ``~/.config``, so this
        tool never touches (or clobbers) their real saved profiles.

        ``PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring`` keeps
        ``profile add``'s secret storage off the real OS keychain the
        same way ``tests/unit/conftest.py``'s own ``fake_keyring``
        fixture does in-process (an in-memory fake there; an env-var
        override here, since a fresh subprocess can't be monkeypatched
        directly) -- not just for isolation, but because the real
        backend can block indefinitely on a GUI authorization prompt in
        a headless context (observed against the real macOS Keychain
        under a freshly-sandboxed ``HOME``). ``null.Keyring`` no-ops
        every operation rather than ``fail.Keyring`` erroring outright
        (which ``profiles/secrets.py``'s own ``_require_keyring()``
        explicitly rejects) -- accepted as "usable" and silently
        discards the secret, which this phase never needs back (``get_
        profile()``/``list_profiles()`` never touch the keyring at all,
        per that module's own docstring)."""
        return {
            "HOME": str(home_dir),
            "XDG_CONFIG_HOME": str(home_dir / ".config"),
            "PYTHON_KEYRING_BACKEND": "keyring.backends.null.Keyring",
        }
