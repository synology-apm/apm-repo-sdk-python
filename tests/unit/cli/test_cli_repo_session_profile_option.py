"""Unit tests for ``--profile`` wired through ``ls``/``doctor``, with a fake
``Session`` and a patched ``store_from_profile``. Both commands open through
``cli.repo_session.opened_repo()``, so the patches target
``cli.repo_session``."""

from __future__ import annotations

import pytest

import synology_apm_repo.cli.repo_session as repo_session
from support.cli import invoke
from support.fakes import faithful_to
from support.model_factories import make_connection
from synology_apm_repo.sdk.api import Catalog, Frame, KeyStatus, Repository, RootFrame
from synology_apm_repo.sdk.format.repo_info import RepoInfo
from synology_apm_repo.sdk.identifiers import CatalogId
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.cli.session_fakes import install_fake_session

_SENTINEL_STORE = object()


async def _fake_store_from_profile(name: str) -> object:
    assert name == "demo"
    return _SENTINEL_STORE


# -- ls --profile ------------------------------------------------------


@faithful_to(Repository)
class _FakeRepo:
    async def catalogs(self) -> list[object]:
        return []

    async def locate(self, ref: NodeRef, *, raw: object = None) -> Frame:
        assert ref.segments == ()  # both tests below invoke ``ls`` on a bare, zero-segment ref
        return RootFrame()


def test_ls_with_profile_opens_with_the_profile_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repo_session, "store_from_profile", _fake_store_from_profile)
    session = install_fake_session(monkeypatch, lambda: [_FakeRepo()])

    invoke(["ls", "--profile", "demo", ""])
    assert len(session.open_calls) == 1
    call = session.open_calls[0]
    assert call["source"] is _SENTINEL_STORE
    assert call["root"] == ""


def test_ls_without_profile_never_calls_store_from_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail_if_called(name: str) -> object:
        raise AssertionError("store_from_profile must not be called without --profile")

    monkeypatch.setattr(repo_session, "store_from_profile", _fail_if_called)
    install_fake_session(monkeypatch, lambda: [_FakeRepo()])

    invoke(["ls", "/some/local/path"])


# -- doctor --profile -----------------------------------------------------

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


@faithful_to(Catalog)
class _FakeCatalog:
    """An ``api.Catalog`` duck-type with what ``doctor``'s ``_catalog_report``
    reads (``workloads()``, ``connection``, ``catalog_id``, ``info``)."""

    def __init__(self, *, catalog_id: str = "1") -> None:
        self.connection = make_connection(
            connection_id="cc-1", display_name="Fake Source", workload_count=0, version_count=0
        )
        self._catalog_id = catalog_id
        self.info = _FAKE_REPO_INFO

    async def workloads(self) -> list[object]:
        return []

    @property
    def catalog_id(self) -> CatalogId:
        return CatalogId(self._catalog_id)


@faithful_to(Repository)
class _FakeDoctorRepo:
    layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="")
    key_status = KeyStatus.NOT_ENCRYPTED
    is_encrypted = False
    key_verification = None

    async def catalogs(self) -> list[object]:
        return [_FakeCatalog()]


def test_doctor_with_profile_resolves_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repo_session, "store_from_profile", _fake_store_from_profile)
    session = install_fake_session(monkeypatch, lambda: [_FakeDoctorRepo()])

    result = invoke(["--verbose", "doctor", "--profile", "demo"])
    assert [call["source"] for call in session.open_calls] == [_SENTINEL_STORE]
    assert "object_store" in result.output


def test_doctor_shows_catalog_id_when_verbose(monkeypatch: pytest.MonkeyPatch) -> None:
    @faithful_to(Repository)
    class _FakeDoctorRepoWithId:
        layout = RepositoryLayout(kind=RepoKind.OBJECT_STORE, repo_root="")
        key_status = KeyStatus.NOT_ENCRYPTED
        is_encrypted = False
        key_verification = None

        async def catalogs(self) -> list[object]:
            return [_FakeCatalog(catalog_id="my-repo-id")]

    monkeypatch.setattr(repo_session, "store_from_profile", _fake_store_from_profile)
    install_fake_session(monkeypatch, lambda: [_FakeDoctorRepoWithId()])

    result = invoke(["--verbose", "doctor", "--profile", "demo"])
    assert "catalog_id=my-repo-id" in result.output


def test_doctor_requires_exactly_one_of_repo_or_profile() -> None:
    result = invoke(["doctor"], exit_code=1)
    assert "exactly one of REPO or --profile" in result.output


def test_doctor_rejects_both_repo_and_profile(tmp_path: object) -> None:
    result = invoke(["doctor", str(tmp_path), "--profile", "demo"], exit_code=1)
    assert "exactly one of REPO or --profile" in result.output
