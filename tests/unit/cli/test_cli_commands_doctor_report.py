"""The ``doctor`` command's report, driven end to end over a fake
``Session``: its human and ``--json`` output in default and ``--verbose``
mode, key status, and how it gathers its catalogs."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from inline_snapshot import snapshot

from support.cli import invoke
from support.model_factories import (
    make_connection,
    make_workload,
)
from synology_apm_repo.sdk.api import Catalog, KeyStatus, KeyVerification, Workload
from synology_apm_repo.sdk.storage.layout import RepoKind, RepositoryLayout
from unit.cli.listing_fakes import (
    FakeCatalog,
    FakeRepo,
)
from unit.cli.session_fakes import install_fake_session


def _alice_catalog() -> FakeCatalog:
    """One backup source holding a supported and an unsupported workload."""
    return FakeCatalog(
        make_connection(
            display_name="Alice-Backup-Source", namespaces=("ns-guid-1",), workload_count=2, version_count=3
        ),
        workloads=[
            make_workload(workload_id=7, display_name="alice@example.com", subtitle="Windows 10 (64-bit) · alice-pc"),
            make_workload(workload_id=8, display_name="alice-gw", workload_type="GW", sub_type="UNRECOGNIZED_SUB_TYPE"),
        ],
    )


def _doctor(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, repo: FakeRepo, *flags: str) -> str:
    install_fake_session(monkeypatch, [repo])
    return invoke([*flags, "doctor", str(tmp_path)]).stdout


def test_default_view_shows_names_counts_and_supported_status_but_no_internal_ids(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    out = _doctor(monkeypatch, tmp_path, FakeRepo(catalogs=[_alice_catalog()]))
    assert out == snapshot("""\
layout: vault
key status: not encrypted

1 backup source
  Alice-Backup-Source — 2 workloads, 3 versions
    - alice@example.com · Windows 10 (64-bit) · alice-pc
    - alice-gw (unsupported)
""")


def test_verbose_adds_repo_root_catalog_and_workload_ids(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = FakeRepo(catalogs=[_alice_catalog()], layout=RepositoryLayout(kind=RepoKind.VAULT, repo_root="/repo"))
    out = _doctor(monkeypatch, tmp_path, repo, "--verbose")
    assert out == snapshot("""\
layout: vault
repo_root: /repo
key status: not encrypted

1 backup source
  Alice-Backup-Source — 2 workloads, 3 versions
    catalog_id=1
    repo_uuid=repo-uuid-1
    repo_type=2
    namespaces=ns-guid-1
    - alice@example.com · Windows 10 (64-bit) · alice-pc
      workload_id=7, workload_type=VM, sub_type=None
    - alice-gw (unsupported)
      workload_id=8, workload_type=GW, sub_type=UNRECOGNIZED_SUB_TYPE
""")


def test_json_keys_each_entry_by_its_stable_fields(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    out = _doctor(monkeypatch, tmp_path, FakeRepo(catalogs=[_alice_catalog()]), "--json")
    assert out == snapshot("""\
{
  "layout": "vault",
  "key": {
    "status": "not_encrypted",
    "is_encrypted": false
  },
  "catalogs": [
    {
      "display_name": "Alice-Backup-Source",
      "workload_count": 2,
      "version_count": 3,
      "workloads": [
        {
          "display_name": "alice@example.com",
          "subtitle": "Windows 10 (64-bit) · alice-pc",
          "supported": true
        },
        {
          "display_name": "alice-gw",
          "subtitle": null,
          "supported": false
        }
      ]
    }
  ]
}
""")


def test_verbose_json_adds_internal_ids(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    out = _doctor(monkeypatch, tmp_path, FakeRepo(catalogs=[_alice_catalog()]), "--verbose", "--json")
    assert out == snapshot("""\
{
  "layout": "vault",
  "key": {
    "status": "not_encrypted",
    "is_encrypted": false
  },
  "catalogs": [
    {
      "display_name": "Alice-Backup-Source",
      "workload_count": 2,
      "version_count": 3,
      "workloads": [
        {
          "display_name": "alice@example.com",
          "subtitle": "Windows 10 (64-bit) · alice-pc",
          "supported": true,
          "workload_id": 7,
          "workload_type": "VM",
          "sub_type": null
        },
        {
          "display_name": "alice-gw",
          "subtitle": null,
          "supported": false,
          "workload_id": 8,
          "workload_type": "GW",
          "sub_type": "UNRECOGNIZED_SUB_TYPE"
        }
      ],
      "catalog_id": "1",
      "namespaces": [
        "ns-guid-1"
      ],
      "repo_uuid": "repo-uuid-1",
      "repo_type": 2
    }
  ],
  "repo_root": "."
}
""")


def test_counts_are_pluralized(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    catalog = FakeCatalog(make_connection(display_name="src", workload_count=1, version_count=2))
    out = _doctor(monkeypatch, tmp_path, FakeRepo(catalogs=[catalog]))
    assert out == snapshot("""\
layout: vault
key status: not encrypted

1 backup source
  src — 1 workload, 2 versions
""")


def test_bracketed_names_and_subtitles_render_literally(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Rich would otherwise parse these as markup tags and drop them.
    catalog = FakeCatalog(
        make_connection(display_name="[limitation] deep_hierarchy"),
        workloads=[make_workload(display_name="[archived] my-vm", subtitle="[old] disk")],
    )
    out = _doctor(monkeypatch, tmp_path, FakeRepo(catalogs=[catalog]))
    assert out == snapshot("""\
layout: vault
key status: not encrypted

1 backup source
  [limitation] deep_hierarchy — 1 workload, 1 version
    - [archived] my-vm · [old] disk
""")


def test_an_invalid_key_shows_its_label_and_gcm_ok(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = FakeRepo(
        key_status=KeyStatus.INVALID,
        is_encrypted=True,
        key_verification=KeyVerification(gcm_ok=False, vault_key=None),
    )
    assert _doctor(monkeypatch, tmp_path, repo) == snapshot("""\
layout: vault
key status: invalid key
  gcm_ok=False

0 backup sources
""")
    assert _doctor(monkeypatch, tmp_path, repo, "--json") == snapshot("""\
{
  "layout": "vault",
  "key": {
    "status": "invalid",
    "is_encrypted": true,
    "gcm_ok": false
  },
  "catalogs": []
}
""")


def test_an_undeterminable_encryption_status_gets_its_own_line(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = FakeRepo(key_status=KeyStatus.NO_KEY_PROVIDED, is_encrypted=None)
    assert _doctor(monkeypatch, tmp_path, repo) == snapshot("""\
layout: vault
key status: key needed
  encryption status could not be determined

0 backup sources
""")


def test_the_ordinary_no_key_case_says_nothing_extra(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = FakeRepo(key_status=KeyStatus.NO_KEY_PROVIDED, is_encrypted=True)
    assert _doctor(monkeypatch, tmp_path, repo) == snapshot("""\
layout: vault
key status: key needed

0 backup sources
""")


class _GatedCatalog(FakeCatalog):
    """``workloads()`` resolves only once all ``total`` siblings have started
    (the last to start sets ``release``): it completes under concurrent
    awaiting and deadlocks under a serial loop."""

    def __init__(self, name: str, *, started: list[str], total: int, release: asyncio.Event) -> None:
        super().__init__(make_connection(display_name=name, workload_count=0, version_count=0))
        self._started = started
        self._total = total
        self._release = release

    async def workloads(self) -> list[Workload]:
        self._started.append(self.connection.display_name)
        if len(self._started) >= self._total:
            self._release.set()
        else:
            await self._release.wait()
        return []


def test_catalogs_are_gathered_concurrently_and_listed_in_repository_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    started: list[str] = []
    release = asyncio.Event()
    # "second" sets the release event, so its workloads() finishes first.
    catalogs: list[Catalog] = [
        _GatedCatalog(name, started=started, total=2, release=release) for name in ("first", "second")
    ]
    out = _doctor(monkeypatch, tmp_path, FakeRepo(catalogs=catalogs))
    # Finishing at all proves concurrency (see _GatedCatalog).
    assert started == ["first", "second"]
    assert out == snapshot("""\
layout: vault
key status: not encrypted

2 backup sources
  first — 0 workloads, 0 versions
  second — 0 workloads, 0 versions
""")


def test_one_failing_catalog_aborts_the_whole_report(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class _RaisingCatalog(FakeCatalog):
        async def workloads(self) -> list[Workload]:
            raise RuntimeError("catalog workloads() blew up")

    catalogs: list[Catalog] = [
        FakeCatalog(make_connection(display_name="ok")),
        _RaisingCatalog(make_connection(connection_config_id=2)),
    ]
    install_fake_session(monkeypatch, [FakeRepo(catalogs=catalogs)])
    result = invoke(["doctor", str(tmp_path)], exit_code=1)
    assert result.stdout == ""
    # The rest of stderr is the traceback.
    assert result.stderr.splitlines()[0] == "internal error: RuntimeError: catalog workloads() blew up"
