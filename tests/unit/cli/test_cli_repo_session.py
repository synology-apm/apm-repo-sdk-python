"""Unit tests for ``synology_apm_repo.cli.repo_session`` — the repo-open
lifecycle shared by ``ls``/``tree``/``cat``/``key``/``verify``/``doctor``."""

from __future__ import annotations

from typing import Any, cast

import pytest
from typer.testing import CliRunner

from synology_apm_repo.cli.main import app
from synology_apm_repo.cli.repo_session import open_single_repo
from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout

runner = CliRunner()

# -- open_single_repo -----------------------------------------------------


class _FakeSession:
    def __init__(self, repos: list[object]) -> None:
        self._repos = repos
        self.open_remote_calls: list[dict[str, object]] = []

    async def open(
        self, fs_path: object, key: object, *, progress: object = None, trace: object = None
    ) -> list[object]:
        return self._repos

    async def open_remote(
        self, store: object, key: object = None, *, root: str = "", progress: object = None, trace: object = None
    ) -> list[object]:
        self.open_remote_calls.append({"store": store, "key": key, "root": root})
        return self._repos


async def test_open_single_repo_returns_the_only_repo() -> None:
    sentinel = object()
    session = cast(Any, _FakeSession([sentinel]))
    assert await open_single_repo(session, "/some/path", None) is sentinel


async def test_open_single_repo_raises_not_found_on_zero_repos() -> None:
    session = cast(Any, _FakeSession([]))
    with pytest.raises(NotFoundError, match="no repository found"):
        await open_single_repo(session, "/some/path", None)


async def test_open_single_repo_with_store_uses_open_remote_with_root() -> None:
    sentinel = object()
    fake_session = _FakeSession([sentinel])
    store = cast(Any, object())
    result = await open_single_repo(cast(Any, fake_session), "sub/path", None, store=store)
    assert result is sentinel
    assert fake_session.open_remote_calls == [{"store": store, "key": None, "root": "sub/path"}]


async def test_open_single_repo_without_store_uses_open() -> None:
    fake_session = _FakeSession([object()])
    await open_single_repo(cast(Any, fake_session), "/some/path", None)
    assert fake_session.open_remote_calls == []


async def test_open_single_repo_raises_not_found_on_multiple_repos() -> None:
    class _FakeRepo:
        def __init__(self, root: str) -> None:
            self.layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root=root)

    session = cast(Any, _FakeSession([_FakeRepo("a"), _FakeRepo("b")]))
    with pytest.raises(NotFoundError, match="2 repositories found"):
        await open_single_repo(session, "/some/path", None)


# -- opened_repo(): friendly_message()'s verbose gate wired end to end -----
#
# open_single_repo()'s own NotFoundError (asserted directly above) is exactly
# the kind of internal-store-path-bearing error opened_repo() must now
# redact by default and restore under --verbose.


class _RaisingSession:
    async def open(self, *args: object, **kwargs: object) -> list[object]:
        # Deliberately doesn't repeat the ref in the message text itself
        # (unlike open_single_repo()'s own NotFoundError above) -- isolates
        # what friendly_message()'s redaction actually strips: the
        # structured "[ref=...]" tag, not incidental text.
        raise NotFoundError("nothing readable at this location", ref="/some/internal/store/path")

    async def close(self) -> None:
        pass


def test_ref_bearing_error_is_redacted_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("synology_apm_repo.cli.repo_session.Session", _RaisingSession)
    result = runner.invoke(app, ["ls", "/some/path"])
    assert result.exit_code == 1
    assert "/some/internal/store/path" not in result.output


def test_ref_bearing_error_is_restored_under_verbose(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("synology_apm_repo.cli.repo_session.Session", _RaisingSession)
    result = runner.invoke(app, ["--verbose", "ls", "/some/path"])
    assert result.exit_code == 1
    assert "/some/internal/store/path" in result.output


class _CrashingSession:
    async def open(self, *args: object, **kwargs: object) -> list[object]:
        raise RuntimeError("boom")  # deliberately not an ApmRepoError -- a bug, not an expected failure

    async def close(self) -> None:
        pass


def test_unexpected_non_apmrepoerror_still_clears_a_live_progress_line(monkeypatch: pytest.MonkeyPatch) -> None:
    """``opened_repo()``'s own ``except ApmRepoError`` doesn't catch this
    at all -- ``finish_live_progress`` must still fire via the shared
    ``finally``, not just that branch's own explicit call, or a bug's own
    traceback would land on top of a dangling progress line instead of a
    clean one."""
    monkeypatch.setattr("synology_apm_repo.cli.repo_session.Session", _CrashingSession)
    result = runner.invoke(app, ["--progress", "always", "ls", "/some/path"])
    assert result.exit_code == 1
    assert "\x1b[2K" in result.stderr
