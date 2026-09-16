"""Shared fixtures for ``tests/unit/`` -- ``fake_keyring`` is used only by
``tests/unit/{cli,sdk}/`` profile tests, never under ``tests/integration/``."""

from __future__ import annotations

from collections.abc import Iterator

import pytest


def _make_in_memory_keyring() -> object:
    """A dict-backed ``keyring.backend.KeyringBackend`` subclass for real
    round-trips in ``profiles/secrets.py`` tests with zero real OS-keyring
    access, mirroring this project's fake-client house style
    (``test_storage_s3.py``'s ``_FakeS3Client``, ``test_storage_azure.py``'s
    mocked ``BlobServiceClient`` — never touch a real backend in a unit
    test). Built lazily to avoid importing ``keyring`` at collection time
    for tests that don't need it."""
    import keyring.backend
    import keyring.errors

    class _InMemoryKeyring(keyring.backend.KeyringBackend):
        priority = 1.0

        def __init__(self) -> None:
            # keyring's own KeyringBackend.__init__ carries no annotations.
            super().__init__()  # type: ignore[no-untyped-call]
            self._values: dict[tuple[str, str], str] = {}

        def get_password(self, service: str, username: str) -> str | None:
            return self._values.get((service, username))

        def set_password(self, service: str, username: str, password: str) -> None:
            self._values[(service, username)] = password

        def delete_password(self, service: str, username: str) -> None:
            try:
                del self._values[(service, username)]
            except KeyError:
                raise keyring.errors.PasswordDeleteError("not found") from None

    return _InMemoryKeyring()


@pytest.fixture
def fake_keyring() -> Iterator[None]:
    """Installs an in-memory keyring backend for the duration of one test,
    restoring whatever backend was active before — see this module's own
    docstring for which tests use it, so they never touch the real OS
    Keychain/Credential Manager/Secret Service."""
    import keyring

    previous = keyring.get_keyring()
    keyring.set_keyring(_make_in_memory_keyring())  # type: ignore[arg-type]
    try:
        yield
    finally:
        keyring.set_keyring(previous)
