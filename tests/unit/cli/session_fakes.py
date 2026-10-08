"""A fake ``Session`` for CLI tests that drive a command through the real
``cli.repo_session.opened_repo()``.

``install_fake_session(monkeypatch, repos)`` patches the ``Session`` the CLI
constructs; its ``open()`` returns ``repos`` (a fresh list from a callable
each call, when one is given) or raises ``open_error``, and records each
call's arguments in ``open_calls``.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, ClassVar

import pytest

from support.fakes import faithful_to
from synology_apm_repo.sdk.api import Session


@faithful_to(Session)
class FakeSession:
    repos: ClassVar[Sequence[object] | Callable[[], Sequence[object]]] = ()
    open_error: ClassVar[BaseException | None] = None
    open_calls: ClassVar[list[dict[str, Any]]]
    close_count: ClassVar[int]

    async def open(
        self, source: object, key: str | None = None, *, root: str = "", progress: object = None, trace: object = None
    ) -> list[object]:
        type(self).open_calls.append({"source": source, "key": key, "root": root, "trace": trace is not None})
        if self.open_error is not None:
            raise self.open_error
        repos = type(self).repos
        return list(repos() if callable(repos) else repos)

    async def close(self) -> None:
        type(self).close_count += 1


def install_fake_session(
    monkeypatch: pytest.MonkeyPatch,
    repos: Sequence[object] | Callable[[], Sequence[object]] = (),
    *,
    open_error: BaseException | None = None,
) -> type[FakeSession]:
    """Patch the CLI's ``Session`` with a fresh ``FakeSession`` subclass and return it."""
    session_cls = type(
        "FakeSession",
        (FakeSession,),
        {
            "repos": staticmethod(repos) if callable(repos) else repos,
            "open_error": open_error,
            "open_calls": [],
            "close_count": 0,
        },
    )
    monkeypatch.setattr("synology_apm_repo.cli.repo_session.Session", session_cls)
    return session_cls
