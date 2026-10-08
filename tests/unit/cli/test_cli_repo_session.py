"""Unit tests for ``synology_apm_repo.cli.repo_session`` — the repo-open
lifecycle shared by ``ls``/``tree``/``cat``/``export``/``key``/``verify``/``doctor``."""

from __future__ import annotations

from typing import Any, ClassVar, cast

import pytest

from support.cli import invoke
from support.fakes import faithful_to
from synology_apm_repo.cli.repo_session import open_single_repo
from synology_apm_repo.sdk.api import Session, TraceEvent
from synology_apm_repo.sdk.errors import NotFoundError
from synology_apm_repo.sdk.storage.layout import RepoKind, RepoLayout
from unit.cli.session_fakes import install_fake_session

# -- open_single_repo -----------------------------------------------------


@faithful_to(Session)
class _FakeSession:
    def __init__(self, repos: list[object]) -> None:
        self._repos = repos
        self.store_open_calls: list[dict[str, object]] = []

    async def open(
        self, source: object, key: object = None, *, root: str = "", progress: object = None, trace: object = None
    ) -> list[object]:
        if not isinstance(source, str):
            self.store_open_calls.append({"store": source, "key": key, "root": root})
        return self._repos


async def test_open_single_repo_returns_the_only_repo() -> None:
    sentinel = object()
    session = cast(Any, _FakeSession([sentinel]))
    assert await open_single_repo(session, "/some/path", None) is sentinel


async def test_open_single_repo_raises_not_found_on_zero_repos() -> None:
    session = cast(Any, _FakeSession([]))
    with pytest.raises(NotFoundError, match="no repository found"):
        await open_single_repo(session, "/some/path", None)


async def test_open_single_repo_with_store_opens_it_with_root() -> None:
    sentinel = object()
    fake_session = _FakeSession([sentinel])
    store = cast(Any, object())
    result = await open_single_repo(cast(Any, fake_session), "sub/path", None, store=store)
    assert result is sentinel
    assert fake_session.store_open_calls == [{"store": store, "key": None, "root": "sub/path"}]


async def test_open_single_repo_without_store_uses_open() -> None:
    fake_session = _FakeSession([object()])
    await open_single_repo(cast(Any, fake_session), "/some/path", None)
    assert fake_session.store_open_calls == []


async def test_open_single_repo_raises_not_found_on_multiple_repos() -> None:
    class _FakeRepo:
        def __init__(self, root: str) -> None:
            self.layout = RepoLayout(kind=RepoKind.OBJECT_STORE, repo_root=root)

    session = cast(Any, _FakeSession([_FakeRepo("a"), _FakeRepo("b")]))
    with pytest.raises(NotFoundError, match="2 repositories found"):
        await open_single_repo(session, "/some/path", None)


# -- opened_repo(): friendly_message()'s verbose gate wired end to end -----
#
# An error's internal store path ("[ref=...]") is redacted by default and
# shown under --verbose.


# The message text omits the ref, so only the structured "[ref=...]" tag can carry it.
_REF_BEARING_ERROR = NotFoundError("nothing readable at this location", ref="/some/internal/store/path")


def test_ref_bearing_error_is_redacted_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_session(monkeypatch, open_error=_REF_BEARING_ERROR)
    result = invoke(["ls", "/some/path"], exit_code=1)
    assert "/some/internal/store/path" not in result.output


def test_ref_bearing_error_is_restored_under_verbose(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_session(monkeypatch, open_error=_REF_BEARING_ERROR)
    result = invoke(["--verbose", "ls", "/some/path"], exit_code=1)
    assert "/some/internal/store/path" in result.output


def test_unexpected_non_apmrepoerror_still_clears_a_live_progress_line(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bug (not an ``ApmRepoError``) still clears the live progress line,
    so its traceback doesn't land on a dangling progress line."""
    install_fake_session(monkeypatch, open_error=RuntimeError("boom"))
    result = invoke(["--progress", "always", "ls", "/some/path"], exit_code=1)
    assert "\x1b[2K" in result.stderr


# -- global flags and --key/--profile, driven through the real opened_repo() --
#
# The commands' own ``opened_repo`` is not replaced here: a recording ``Session`` stands in for the SDK,
# so what the flags actually hand to ``Session.open`` is what is asserted.


@faithful_to(Session)
class _RecordingSession:
    """Records what ``opened_repo()`` asks of the SDK, then finds no repository (so the command exits 1)."""

    calls: ClassVar[list[tuple[str, dict[str, object]]]] = []

    async def open(
        self, source: object, key: object = None, *, root: str = "", progress: object = None, trace: object = None
    ) -> list[object]:
        if not isinstance(source, str):
            _RecordingSession.calls.append(("open_store", {"store": source, "key": key, "root": root}))
            return []
        _RecordingSession.calls.append(("open", {"fs_path": source, "key": key, "trace": trace is not None}))
        if callable(trace):
            trace(TraceEvent(method="read", path="Pool/0/0.buk", offset=0, length=64, result_length=64, elapsed=0.0015))
        return []

    async def close(self) -> None:
        pass


@pytest.fixture
def recording(monkeypatch: pytest.MonkeyPatch) -> type[_RecordingSession]:
    _RecordingSession.calls = []
    monkeypatch.setattr("synology_apm_repo.cli.repo_session.Session", _RecordingSession)
    return _RecordingSession


_KEY = "AliceKey0001@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
# ls/tree/cat take the ref as the argument; verify/doctor take the repository path.
_REPO_COMMANDS = [
    ["ls", "/repo#Source"],
    ["tree", "/repo#Source"],
    ["cat", "/repo#Source/Workload/Version/Disk/file.bin"],
    ["verify", "/repo"],
    ["doctor", "/repo"],
]


@pytest.mark.parametrize("command", _REPO_COMMANDS, ids=lambda command: command[0])
def test_key_reaches_session_open(recording: type[_RecordingSession], command: list[str]) -> None:
    result = invoke([*command, "--key", _KEY], exit_code=1)
    assert result.stdout == ""
    assert result.stderr == "error: no repository found at '/repo'\n"
    assert recording.calls == [("open", {"fs_path": "/repo", "key": _KEY, "trace": False})]


@pytest.mark.parametrize("command", _REPO_COMMANDS, ids=lambda command: command[0])
def test_without_key_session_open_gets_none(recording: type[_RecordingSession], command: list[str]) -> None:
    invoke(command, exit_code=1)

    assert recording.calls == [("open", {"fs_path": "/repo", "key": None, "trace": False})]


@pytest.mark.parametrize(
    "command",
    [
        ["ls", "sub/dir#Source", "--profile", "work"],
        ["tree", "sub/dir#Source", "--profile", "work"],
        ["cat", "sub/dir#Source/W/V/D/file.bin", "--profile", "work"],
    ],
    ids=lambda command: command[0],
)
def test_profile_opens_the_repository_through_the_profiles_store(
    recording: type[_RecordingSession], monkeypatch: pytest.MonkeyPatch, command: list[str]
) -> None:
    store = object()
    resolved: list[str] = []

    async def resolve(name: str) -> object:
        resolved.append(name)
        return store

    monkeypatch.setattr("synology_apm_repo.cli.repo_session.store_from_profile", resolve)

    invoke([*command, "--key", _KEY], exit_code=1)
    assert resolved == ["work"]
    # With --profile the path before '#' is store-relative, not a filesystem path.
    assert recording.calls == [("open_store", {"store": store, "key": _KEY, "root": "sub/dir"})]


@pytest.mark.parametrize("command", [["verify"], ["doctor"], ["key", "--key", _KEY]], ids=lambda command: command[0])
def test_profile_replaces_the_repository_argument(
    recording: type[_RecordingSession], monkeypatch: pytest.MonkeyPatch, command: list[str]
) -> None:
    store = object()

    async def resolve(name: str) -> object:
        return store

    monkeypatch.setattr("synology_apm_repo.cli.repo_session.store_from_profile", resolve)

    invoke([*command, "--profile", "work"], exit_code=1)
    assert [call[0] for call in recording.calls] == ["open_store"]
    assert recording.calls[0][1]["store"] is store
    assert recording.calls[0][1]["root"] == ""


@pytest.mark.parametrize("flags", [[], ["--json"], ["--verbose"]], ids=["none", "json", "verbose"])
def test_trace_is_off_unless_asked_for(recording: type[_RecordingSession], flags: list[str]) -> None:
    result = invoke([*flags, "ls", "/repo"], exit_code=None)

    assert recording.calls == [("open", {"fs_path": "/repo", "key": None, "trace": False})]
    assert "[trace]" not in result.stderr and '"method"' not in result.stderr


def test_trace_prints_one_line_per_store_call_on_stderr_only(recording: type[_RecordingSession]) -> None:
    result = invoke(["--trace", "ls", "/repo"], exit_code=None)

    assert recording.calls[0][1]["trace"] is True
    assert result.stdout == ""
    assert result.stderr == (
        "[trace] read     Pool/0/0.buk offset=0 length=64 -> 64 (1.50ms)\nerror: no repository found at '/repo'\n"
    )


def test_trace_with_json_prints_ndjson_events_on_stderr(recording: type[_RecordingSession]) -> None:
    result = invoke(["--json", "--trace", "ls", "/repo"], exit_code=None)

    assert result.stdout == ""
    assert result.stderr.splitlines()[0] == (
        '{"method": "read", "path": "Pool/0/0.buk", "offset": 0, "length": 64, "result_length": 64, "elapsed": 0.0015, '
        '"error": null}'
    )
    assert result.stderr.splitlines()[1] == "error: no repository found at '/repo'"
