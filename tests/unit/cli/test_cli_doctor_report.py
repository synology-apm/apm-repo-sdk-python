"""Unit tests for ``synology_apm_repo.cli.commands.doctor``'s report
builders (``_workload_report``/``_catalog_report``) and ``_render_human`` —
synthetic ``Connection``/``Workload`` objects, key-status disambiguation,
verbose-only internal-id gating, and Rich-markup-literal rendering."""

from __future__ import annotations

from typing import Any, cast

import pytest

from synology_apm_repo.cli.commands.doctor import _catalog_report, _render_human, _workload_report
from synology_apm_repo.sdk.api import Connection, Workload
from synology_apm_repo.sdk.format.repo_info import RepoInfo
from synology_apm_repo.sdk.identifiers import CatalogId, ConnectionConfigId, ConnectionId, WorkloadId, WorkloadUid

_SENSITIVE_WORKLOAD_NAME = "alice@customer-corp.example.com"
_SENSITIVE_SUBTITLE = "Windows 10 (64-bit) · customer-hostname-01"
_SENSITIVE_CONNECTION_NAME = "CustomerCorp-Backup-Source"


def _workload() -> Workload:
    return Workload(
        workload_id=WorkloadId(1),
        workload_uid=WorkloadUid("wuid"),
        workload_type="VM",
        sub_type=None,
        display_name=_SENSITIVE_WORKLOAD_NAME,
        subtitle=_SENSITIVE_SUBTITLE,
        spec={},
    )


def _connection() -> Connection:
    return Connection(
        connection_config_id=ConnectionConfigId(1),
        connection_id=ConnectionId("cc"),
        display_name=_SENSITIVE_CONNECTION_NAME,
        namespaces=("customer-namespace-guid",),
        workload_count=1,
        version_count=1,
    )


class _FakeRepo:
    """A minimal stand-in for ``api.repository.Repository``, exposing just
    the one method ``_workload_report`` actually calls
    (``workload_is_supported()``) — simpler than monkeypatching the real
    class."""

    def workload_is_supported(self, workload: Workload) -> bool:
        return workload.workload_type in ("VM", "PC", "PS", "FS")


class _FakeCatalog:
    """A minimal stand-in for ``api.repository.Catalog``, exposing just
    what ``_catalog_report`` actually reads (``connection``, ``catalog_id``,
    ``info``, ``workloads()``)."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.catalog_id = CatalogId("catalog-1")
        self.info = RepoInfo(
            uuid="u",
            major=1,
            minor=0,
            repo_type=2,
            repo_flag=None,
            is_global_dedup_supported=None,
            is_worm_supported=None,
            compress_algorithm=None,
            encrypt_algorithm=None,
            raw={},
        )

    async def workloads(self) -> list[Workload]:
        return [_workload()]


class TestWorkloadReport:
    def test_keeps_the_real_display_name_and_subtitle(self) -> None:
        report = _workload_report(_FakeRepo(), _workload(), verbose=False)  # type: ignore[arg-type]
        assert report["display_name"] == _SENSITIVE_WORKLOAD_NAME
        assert report["subtitle"] == _SENSITIVE_SUBTITLE

    def test_supported_flag_comes_from_repo_workload_is_supported(self) -> None:
        report = _workload_report(_FakeRepo(), _workload(), verbose=False)  # type: ignore[arg-type]
        assert report["supported"] is True  # VM, per _FakeRepo.workload_is_supported

    def test_unsupported_flag_also_comes_from_repo_workload_is_supported(self) -> None:
        wl = Workload(
            workload_id=WorkloadId(1),
            workload_uid=WorkloadUid("w"),
            workload_type="GW",
            sub_type="UNRECOGNIZED_SUB_TYPE",
            display_name="x",
            subtitle=None,
            spec={},
        )
        report = _workload_report(_FakeRepo(), wl, verbose=False)  # type: ignore[arg-type]
        assert report["supported"] is False  # GW/UNRECOGNIZED_SUB_TYPE, per _FakeRepo.workload_is_supported


class TestCatalogReport:
    """``_catalog_report`` is ``async def`` (it awaits ``catalog.workloads()``);
    ``_workload_report`` above stays synchronous — formatting plus one
    plain, no-I/O ``repo.workload_is_supported()`` call."""

    async def test_keeps_the_real_display_name_and_counts(self) -> None:
        report = await _catalog_report(_FakeRepo(), _FakeCatalog(_connection()), verbose=False)  # type: ignore[arg-type]
        assert report["display_name"] == _SENSITIVE_CONNECTION_NAME
        assert report["workload_count"] == 1
        assert report["version_count"] == 1

    async def test_verbose_adds_catalog_id_namespaces_and_repo_info(self) -> None:
        report = await _catalog_report(_FakeRepo(), _FakeCatalog(_connection()), verbose=True)  # type: ignore[arg-type]
        assert report["catalog_id"] == "catalog-1"
        assert report["namespaces"] == ["customer-namespace-guid"]
        assert report["repo_uuid"] == "u"
        assert report["repo_type"] == 2


def test_render_human_shows_gcm_ok_when_the_key_is_invalid(capsys: pytest.CaptureFixture[str]) -> None:
    report: object = {
        "layout": "object_store",
        "key": {"status": "invalid", "is_encrypted": True, "gcm_ok": False},
        "catalogs": [],
    }
    _render_human(cast(Any, report), verbose=False)
    out = capsys.readouterr().out
    assert "key status" in out
    assert "gcm_ok=False" in out


def test_render_human_disambiguates_unresolved_encryption_status(capsys: pytest.CaptureFixture[str]) -> None:
    """``status=no_key_provided`` alone is ambiguous — it covers both
    "confirmed encrypted, no key given yet" (``is_encrypted`` is ``True``)
    and the rare "couldn't tell" case (``is_encrypted`` is ``None``, see
    ``ARCHITECTURE.md``'s Repository Layer section). Only the latter gets
    the extra disambiguating line."""
    report: object = {
        "layout": "object_store",
        "key": {"status": "no_key_provided", "is_encrypted": None},
        "catalogs": [],
    }
    _render_human(cast(Any, report), verbose=False)
    assert "could not be determined" in capsys.readouterr().out


def test_render_human_says_nothing_extra_for_the_ordinary_no_key_case(capsys: pytest.CaptureFixture[str]) -> None:
    report: object = {
        "layout": "object_store",
        "key": {"status": "no_key_provided", "is_encrypted": True},
        "catalogs": [],
    }
    _render_human(cast(Any, report), verbose=False)
    assert "could not be determined" not in capsys.readouterr().out


def test_render_human_shows_internal_ids_when_verbose(capsys: pytest.CaptureFixture[str]) -> None:
    """``_build_report``/``_catalog_report``/``_workload_report`` already
    compute ``catalog_id``/``repo_uuid``/``repo_type``/
    ``namespaces``/``workload_id``/``workload_type``/``sub_type`` only
    ``if state.verbose`` — this pins down that ``_render_human`` (not just
    ``--json --verbose``) actually shows them, not just computes and
    discards them. The verbose-only fields are present in ``report`` at
    all (rather than a separate ``verbose`` check re-deriving the same
    gate) — see ``_DoctorReport``'s own docstring."""
    report: object = {
        "layout": "vault",
        "repo_root": "/repo",
        "key": {"status": "not_encrypted", "is_encrypted": False},
        "catalogs": [
            {
                "display_name": "Source 1",
                "workload_count": 1,
                "version_count": 1,
                "catalog_id": "cat-42",
                "namespaces": ["ns-guid-1"],
                "repo_uuid": "u",
                "repo_type": 2,
                "workloads": [
                    {
                        "display_name": "my-vm",
                        "subtitle": None,
                        "supported": True,
                        "workload_id": 7,
                        "workload_type": "VM",
                        "sub_type": None,
                    }
                ],
            }
        ],
    }
    _render_human(cast(Any, report), verbose=True)
    out = capsys.readouterr().out
    assert "repo_root" in out and "/repo" in out
    assert "catalog_id=cat-42" in out
    assert "repo_uuid=u" in out
    assert "repo_type=2" in out
    assert "namespaces=ns-guid-1" in out
    assert "workload_id=7" in out
    assert "workload_type=VM" in out
    assert "sub_type=None" in out


def test_render_human_renders_a_bracketed_display_name_and_subtitle_literally(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Real content Rich would otherwise mistake for a markup tag and
    # silently drop (e.g. "[limitation] deep_hierarchy" -> "
    # deep_hierarchy") -- must render in full, byte-for-byte.
    report: object = {
        "layout": "vault",
        "key": {"status": "not_encrypted", "is_encrypted": False},
        "catalogs": [
            {
                "display_name": "[limitation] deep_hierarchy",
                "workload_count": 1,
                "version_count": 1,
                "workloads": [
                    {"display_name": "[archived] my-vm", "subtitle": "[old] disk", "supported": True},
                ],
            }
        ],
    }
    _render_human(cast(Any, report), verbose=False)
    out = capsys.readouterr().out
    assert "[limitation] deep_hierarchy" in out
    assert "[archived] my-vm" in out
    assert "[old] disk" in out


def test_render_human_omits_internal_ids_when_not_verbose(capsys: pytest.CaptureFixture[str]) -> None:
    report: object = {
        "layout": "vault",
        "key": {"status": "not_encrypted", "is_encrypted": False},
        "catalogs": [
            {
                "display_name": "Source 1",
                "workload_count": 1,
                "version_count": 1,
                "workloads": [{"display_name": "my-vm", "subtitle": None, "supported": True}],
            }
        ],
    }
    _render_human(cast(Any, report), verbose=False)
    out = capsys.readouterr().out
    assert "repo_uuid" not in out
    assert "repo_type" not in out
    assert "catalog_id" not in out
    assert "workload_id" not in out


__all__: list[str] = []
