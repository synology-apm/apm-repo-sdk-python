"""Unit tests for ``--profile`` wired through ``ls``/``doctor`` — a fake
``Session``/``detect_layout`` stands in for real storage, and
``resolve_profile_store`` is monkeypatched so no real profile, keyring, or
network access is needed. Both ``ls`` and ``doctor`` are built on
``cli.repo_session.opened_repo()``, so both commands' own
``Session``/``resolve_profile_store`` patches target ``cli.repo_session``,
not the command modules themselves."""

from __future__ import annotations

from typing import Any, cast

import pytest
from typer.testing import CliRunner

import synology_apm_repo.cli.repo_session as repo_session
from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.api import Connection, Frame, KeyStatus
from synology_apm_repo.sdk.format.repo_info import RepoInfo
from synology_apm_repo.sdk.identifiers import CatalogId, ConnectionConfigId, ConnectionId
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout

runner = CliRunner()

_SENTINEL_STORE = object()


async def _fake_resolve_profile_store(name: str) -> object:
    assert name == "demo"
    return _SENTINEL_STORE


# -- ls --profile ------------------------------------------------------


class _FakeRepo:
    async def catalogs(self) -> list[object]:
        return []

    async def walk_human_ref(self, segments: tuple[str, ...], *, object_db_id: str | None = None) -> Frame:
        assert segments == ()  # both tests below invoke ``ls`` on a bare, zero-segment ref
        return Frame(level="root")


class _FakeSession:
    def __init__(self) -> None:
        self.open_remote_calls: list[dict[str, object]] = []

    async def open_remote(self, store: object, key: object = None, *, root: str = "", **kwargs: object) -> list[object]:
        self.open_remote_calls.append({"store": store, "key": key, "root": root})
        return [_FakeRepo()]

    async def close(self) -> None:
        pass


def test_ls_with_profile_opens_via_open_remote(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_session = _FakeSession()
    monkeypatch.setattr(repo_session, "resolve_profile_store", _fake_resolve_profile_store)
    monkeypatch.setattr(repo_session, "Session", lambda: fake_session)

    result = runner.invoke(app, ["ls", "--profile", "demo", ""])
    assert result.exit_code == 0, result.output
    assert len(fake_session.open_remote_calls) == 1
    call = fake_session.open_remote_calls[0]
    assert call["store"] is _SENTINEL_STORE
    assert call["root"] == ""


def test_ls_without_profile_never_calls_resolve_profile_store(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail_if_called(name: str) -> object:
        raise AssertionError("resolve_profile_store must not be called without --profile")

    monkeypatch.setattr(repo_session, "resolve_profile_store", _fail_if_called)

    class _FakeLocalSession:
        async def open(self, *args: object, **kwargs: object) -> list[object]:
            return [_FakeRepo()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr(repo_session, "Session", lambda: cast(Any, _FakeLocalSession()))

    result = runner.invoke(app, ["ls", "/some/local/path"])
    assert result.exit_code == 0, result.output


# -- doctor --profile -----------------------------------------------------
# doctor's own --verbose output surfaces repo_root (repository-wide) plus
# each catalog's own catalog_id/namespaces/repo_uuid/repo_type.

_FAKE_REPO_INFO = RepoInfo(
    uuid="fake-uuid",
    major=1,
    minor=0,
    repo_type=None,
    repo_flag=None,
    is_global_dedup_supported=None,
    is_worm_supported=None,
    compress_algorithm=None,
    encrypt_algorithm=None,
    raw={},
)


class _FakeCatalog:
    """Duck-typed stand-in for ``api.Catalog`` — just enough surface for
    ``doctor``'s own ``_catalog_report`` to read (``workloads()``,
    ``connection``, ``catalog_id``, ``info``)."""

    def __init__(self, *, catalog_id: str = "1") -> None:
        self.connection = Connection(
            connection_config_id=ConnectionConfigId(1),
            connection_id=ConnectionId("cc-1"),
            display_name="Fake Source",
            namespaces=(),
            workload_count=0,
            version_count=0,
        )
        self._catalog_id = catalog_id
        self.info = _FAKE_REPO_INFO

    async def workloads(self) -> list[object]:
        return []

    @property
    def catalog_id(self) -> CatalogId:
        return CatalogId(self._catalog_id)


class _FakeDoctorRepo:
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="")
    key_status = KeyStatus.NOT_ENCRYPTED
    is_encrypted = False
    key_verification = None

    async def catalogs(self) -> list[object]:
        return [_FakeCatalog()]


class _FakeDoctorSession:
    def __init__(self) -> None:
        self.open_remote_calls: list[dict[str, object]] = []

    async def open_remote(self, store: object, key: object = None, *, root: str = "", **kwargs: object) -> list[object]:
        assert store is _SENTINEL_STORE
        self.open_remote_calls.append({"store": store, "key": key, "root": root})
        return [_FakeDoctorRepo()]

    async def close(self) -> None:
        pass


def test_doctor_with_profile_resolves_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repo_session, "resolve_profile_store", _fake_resolve_profile_store)
    monkeypatch.setattr(repo_session, "Session", lambda: _FakeDoctorSession())

    result = runner.invoke(app, ["--verbose", "doctor", "--profile", "demo"])
    assert result.exit_code == 0, result.output
    assert "object_store" in result.output


def test_doctor_shows_catalog_id_when_verbose(monkeypatch: pytest.MonkeyPatch) -> None:
    """``catalog_id`` (an object-storage catalog's own repo-id, or a
    vault's ``connection_config_id`` stringified) is the
    ``--verbose``-only per-catalog identifier asserted on here, since a
    repository's catalogs can each have their own."""

    class _FakeDoctorRepoWithId:
        layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="")
        key_status = KeyStatus.NOT_ENCRYPTED
        is_encrypted = False
        key_verification = None

        async def catalogs(self) -> list[object]:
            return [_FakeCatalog(catalog_id="my-repo-id")]

    class _FakeDoctorSessionWithId:
        async def open_remote(
            self, store: object, key: object = None, *, root: str = "", **kwargs: object
        ) -> list[object]:
            return [_FakeDoctorRepoWithId()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr(repo_session, "resolve_profile_store", _fake_resolve_profile_store)
    monkeypatch.setattr(repo_session, "Session", lambda: _FakeDoctorSessionWithId())

    result = runner.invoke(app, ["--verbose", "doctor", "--profile", "demo"])
    assert result.exit_code == 0, result.output
    assert "catalog_id=my-repo-id" in result.output


def test_doctor_requires_exactly_one_of_repo_or_profile() -> None:
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 1
    assert "exactly one of REPO or --profile" in result.output


def test_doctor_rejects_both_repo_and_profile(tmp_path: object) -> None:
    result = runner.invoke(app, ["doctor", str(tmp_path), "--profile", "demo"])
    assert result.exit_code == 1
    assert "exactly one of REPO or --profile" in result.output


__all__: list[str] = []
