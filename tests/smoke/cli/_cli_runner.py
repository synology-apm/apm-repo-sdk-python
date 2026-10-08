"""Subprocess wrapper around the installed ``synology-apm-repo-cli`` console
script; the pytest suite only ever runs the CLI in-process.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

#: Default wall-clock budget (seconds) for one invocation.
_DEFAULT_TIMEOUT = 120


def _decode(raw: bytes) -> str:
    """Lossy decode for report display: ``cat``'s stdout can be binary."""
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
    """Invokes ``synology-apm-repo-cli`` via ``uv run``, always with
    ``--no-input`` (no call blocks on a prompt) and ``PAGER=""`` (the CLI's
    "never page" signal, ``cli/paging.py``).

    Output is captured as bytes: ``CliResult.stdout``/``.stderr`` are
    lossily decoded for display, ``.stdout_bytes`` is the real byte count.
    ``base_env`` is merged into every call's environment, under each call's
    ``env_overrides``.
    """

    def __init__(self, base_env: dict[str, str] | None = None) -> None:
        self.base_env: dict[str, str] = dict(base_env or {})

    def run(
        self,
        *args: str,
        env_overrides: dict[str, str] | None = None,
        timeout: float = _DEFAULT_TIMEOUT,
        input_text: str | None = None,
    ) -> CliResult:
        argv = ["uv", "run", "synology-apm-repo-cli", "--no-input", *args]
        env = {**os.environ, "PAGER": "", **self.base_env, **(env_overrides or {})}
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
        """Start the subprocess, wait ``cancel_after`` seconds, then send one
        ``SIGINT``."""
        argv = ["uv", "run", "synology-apm-repo-cli", "--no-input", *args]
        env = {**os.environ, "PAGER": "", **self.base_env, **(env_overrides or {})}
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
        trip: config under ``home_dir`` instead of the real ``~/.config``,
        and the ``null`` keyring backend, which ``profiles/secrets.py``
        accepts (unlike ``fail``) and which discards secrets -- the real OS
        keychain can block on a GUI prompt, and this phase never reads a
        secret back."""
        return {
            "HOME": str(home_dir),
            "XDG_CONFIG_HOME": str(home_dir / ".config"),
            "PYTHON_KEYRING_BACKEND": "keyring.backends.null.Keyring",
        }

    @staticmethod
    def file_keyring_env(config_dir: Path, keyring_file: Path) -> dict[str, str]:
        """Environment that points the CLI's profile config at ``config_dir``
        and its secrets at ``keyring_file`` (``tests/smoke/_file_keyring.py``),
        leaving ``HOME`` alone so ``uv run`` keeps its own cache."""
        return {
            "XDG_CONFIG_HOME": str(config_dir),
            "PYTHON_KEYRING_BACKEND": "_file_keyring.FileKeyring",
            "SMOKE_KEYRING_FILE": str(keyring_file),
            "PYTHONPATH": os.pathsep.join(
                filter(None, [str(Path(__file__).resolve().parents[1]), os.environ.get("PYTHONPATH", "")])
            ),
        }
