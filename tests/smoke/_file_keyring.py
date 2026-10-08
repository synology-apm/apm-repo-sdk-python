"""File-backed ``keyring`` backend for a smoke run's own throwaway profiles.

Loaded by a fresh ``synology-apm-repo-cli`` subprocess via
``PYTHON_KEYRING_BACKEND=_file_keyring.FileKeyring`` (``PYTHONPATH`` pointing
at this directory), so secrets live in the JSON file named by
``SMOKE_KEYRING_FILE`` -- inside a temp dir the caller deletes -- and never
reach the OS keychain.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from keyring.backend import KeyringBackend
from keyring.errors import PasswordDeleteError


class FileKeyring(KeyringBackend):
    priority = 1

    @staticmethod
    def _path() -> Path:
        return Path(os.environ["SMOKE_KEYRING_FILE"])

    def _load(self) -> dict[str, str]:
        path = self._path()
        return json.loads(path.read_text()) if path.exists() else {}

    def _store(self, data: dict[str, str]) -> None:
        fd = os.open(self._path(), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)

    def get_password(self, service: str, username: str) -> str | None:
        return self._load().get(f"{service}\n{username}")

    def set_password(self, service: str, username: str, password: str) -> None:
        data = self._load()
        data[f"{service}\n{username}"] = password
        self._store(data)

    def delete_password(self, service: str, username: str) -> None:
        data = self._load()
        if data.pop(f"{service}\n{username}", None) is None:
            raise PasswordDeleteError(f"no password for {service!r}")
        self._store(data)
