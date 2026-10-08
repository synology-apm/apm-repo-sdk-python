"""Fixtures shared across ``tests/unit/``."""

from __future__ import annotations

from collections.abc import Iterator

import pytest


def _make_in_memory_keyring() -> object:
    """A dict-backed ``keyring.backend.KeyringBackend``, built lazily so
    collection doesn't import ``keyring`` for tests that never use it."""
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
    restoring whatever backend was active before, so it never touches the
    real OS Keychain/Credential Manager/Secret Service."""
    import keyring

    previous = keyring.get_keyring()
    keyring.set_keyring(_make_in_memory_keyring())  # type: ignore[arg-type]
    try:
        yield
    finally:
        keyring.set_keyring(previous)
