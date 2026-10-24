"""Subprocess wrapper around the installed ``synology-apm-repo-cli`` console
script; the pytest suite only ever runs the CLI in-process.
"""

from __future__ import annotations

import contextlib
import locale
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

#: Default wall-clock budget (seconds) for one invocation.
_DEFAULT_TIMEOUT = 120

#: Seconds for a kill to finish, then again to read what the killed
#: invocation already wrote.
_DRAIN_AFTER_KILL_TIMEOUT = 5


def _stdio_encoding(env: dict[str, str]) -> str:
    """The encoding a Python child started with ``env`` uses for its piped
    stdin/stdout/stderr: ``PYTHONIOENCODING``, else UTF-8 in UTF-8 mode
    (``PYTHONUTF8``, on by default from Python 3.15), else the locale's (the
    ANSI code page on Windows). ``uv run`` starts the CLI on this process's
    own interpreter version."""
    configured = env.get("PYTHONIOENCODING", "").partition(":")[0]
    if configured:
        return configured
    utf8_mode = env.get("PYTHONUTF8", "1" if sys.version_info >= (3, 15) else "0") == "1"
    return "utf-8" if utf8_mode else locale.getencoding()


def _decode(raw: bytes, encoding: str) -> str:
    """Lossy decode for report display: ``cat``'s stdout can be binary."""
    return raw.decode(encoding, errors="replace")


def _kill_group(proc: subprocess.Popen[bytes]) -> None:
    """Kills ``proc`` and every process under it (its process group on
    POSIX, its process tree on Windows) and reaps ``proc``: killing
    ``uv run``'s pid alone orphans the CLI process it started, which then
    runs on with nobody reading its output."""
    if sys.platform == "win32":
        try:
            subprocess.run(
                ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                capture_output=True,
                check=False,
                timeout=_DRAIN_AFTER_KILL_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            proc.kill()
    else:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
    proc.wait()


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
        encoding = _stdio_encoding(env)
        input_bytes = input_text.encode(encoding) if input_text is not None else None
        proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE if input_bytes is not None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            start_new_session=True,
        )
        timed_out = False
        try:
            stdout_raw, stderr_raw = proc.communicate(input_bytes, timeout=timeout)
        except subprocess.TimeoutExpired:
            _kill_group(proc)
            try:
                stdout_raw, stderr_raw = proc.communicate(timeout=_DRAIN_AFTER_KILL_TIMEOUT)
            except subprocess.TimeoutExpired:
                # A descendant outside the killed group still holds the pipes.
                stdout_raw, stderr_raw = b"", b""
            timed_out = True
        except BaseException:
            _kill_group(proc)
            raise
        return CliResult(
            list(args),
            exit_code=-1 if timed_out else proc.returncode,
            stdout=_decode(stdout_raw, encoding),
            stderr=_decode(stderr_raw, encoding),
            stdout_bytes=len(stdout_raw),
            timed_out=timed_out,
        )

    def run_cancellable(
        self,
        *args: str,
        env_overrides: dict[str, str] | None = None,
        cancel_on: str,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> CliResult:
        """Start the subprocess and send one ``SIGINT`` once ``cancel_on``
        first appears on its stderr; never sent if the process exits first.

        The subprocess runs in its own process group and the signal goes to
        that whole group, as a terminal's Ctrl-C does: sent to ``uv run``'s
        pid alone, it can be lost before the CLI's own handler is installed.
        POSIX only: Windows delivers no Ctrl-C to another process group.
        """
        if sys.platform == "win32":
            raise NotImplementedError("run_cancellable needs POSIX process groups")
        argv = ["uv", "run", "synology-apm-repo-cli", "--no-input", *args]
        env = {**os.environ, "PAGER": "", **self.base_env, **(env_overrides or {})}
        proc: subprocess.Popen[bytes] = subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, start_new_session=True
        )
        assert proc.stdout is not None and proc.stderr is not None
        encoding = _stdio_encoding(env)
        marker = cancel_on.encode(encoding)
        seen = threading.Event()
        stdout = bytearray()
        stderr = bytearray()

        def _drain(fd: int, sink: bytearray, watch: bool) -> None:
            while chunk := os.read(fd, 65536):
                sink += chunk
                if watch and marker in sink:
                    seen.set()

        readers = [
            threading.Thread(target=_drain, args=(proc.stdout.fileno(), stdout, False), daemon=True),
            threading.Thread(target=_drain, args=(proc.stderr.fileno(), stderr, True), daemon=True),
        ]
        for reader in readers:
            reader.start()
        deadline = time.monotonic() + timeout
        while not seen.wait(0.01) and proc.poll() is None and time.monotonic() < deadline:
            pass
        if seen.is_set() and proc.poll() is None:
            os.killpg(proc.pid, signal.SIGINT)
        try:
            proc.wait(timeout=max(deadline - time.monotonic(), 0))
            timed_out = False
        except subprocess.TimeoutExpired:
            _kill_group(proc)
            timed_out = True
        for reader in readers:
            reader.join()
        return CliResult(
            list(args),
            exit_code=-1 if timed_out else proc.returncode,
            stdout=_decode(bytes(stdout), encoding),
            stderr=_decode(bytes(stderr), encoding),
            stdout_bytes=len(stdout),
            timed_out=timed_out,
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
