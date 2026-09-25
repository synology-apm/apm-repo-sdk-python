"""Regression test for ``synology_apm_repo.sdk.units.saas.mail``
against real GWS/M365 Mail workloads — replayed from committed fixtures,
with **no external dependency**: these always run, on CI or anywhere
else, because they go through ``ReplayStore`` instead of a real
``LocalFsStore``.

Deliberately narrow: this module only proves that a real GWS/M365 Mail
workload dispatches to ``MailProvider`` and resolves its real folder
structure correctly — it never lists or reads an individual real
message. A message's own Subject *is* its content (unlike a Device/FS/
Drive node, whose name is just a filename), and reading it would make
``RecordingStore`` capture that real content into the committed fixture
regardless of what the test then asserts — narrowing what a test
*asserts* can't undo that. ``build_eml()``'s ``X-ABL-ID`` reassembly,
per-folder listing, pagination, and M365 folder-name resolution are all
covered synthetically by ``tests/unit/sdk/test_units_saas_mail.py``
instead.

The fixtures (``tests/fixtures/``, recorded against a real store rooted at
``apv-sample-1/@ActiveProtectVault`` — see ``tests/CLAUDE.md``'s
"Recording a fixture" section for the ``pytest --record-against=...``/
``make record-fixture`` workflow that (re-)records these):

- ``units_saas_mail_gws_apv1.json.gz`` — a real GWS Mail workload
  (workload_id 5): just the index resolution and the top-level "Mail"
  bucket's own existence, not its contents.
- ``units_saas_mail_m365_apv1.json.gz`` — a real M365 Exchange Mail
  workload (workload_id 19): just the index resolution and its real,
  nested ``mail_folder_table`` hierarchy's own existence/shape, not any
  individual folder's or message's content.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.catalog.connection import connections
from synology_apm_repo.sdk.catalog.version import versions
from synology_apm_repo.sdk.catalog.workload import workloads
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.layout import detect_layout
from synology_apm_repo.sdk.units.saas.mail import MailProvider
from synology_apm_repo.sdk.units.saas.provider import SaasWorkloadProvider
from synology_apm_repo.sdk.units.saas.stream import SaasStreamCache


async def _open_provider(repo: DedupRepo, saas_streams: SaasStreamCache, workload_id: int) -> SaasWorkloadProvider:
    all_workloads = [w for c in await connections(repo) for w in await workloads(repo, c)]
    workload = next(w for w in all_workloads if w.workload_id == workload_id)
    version = (await versions(repo, workload))[-1]  # latest
    return await MailProvider(repo, version, saas_streams)


async def test_replayed_gws_mail_workload_resolves_to_its_mail_bucket(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_mail_gws_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_provider(repo, saas_streams, workload_id=5)
        try:
            top = await provider.children(provider.root())
            assert [n.name for n in top] == ["Mail"]
        finally:
            await provider.close()


async def test_replayed_m365_mail_workload_resolves_to_its_real_folder_hierarchy(
    record_target: Callable[..., Awaitable[ObjectStore]],
) -> None:
    store = await record_target("units_saas_mail_m365_apv1.json.gz", allow_content=True)
    layout = await detect_layout(store)
    async with await DedupRepo.open(store, layout) as repo, SaasStreamCache(repo) as saas_streams:
        provider = await _open_provider(repo, saas_streams, workload_id=19)
        try:
            top = await provider.children(provider.root())
            # The real mail_folder_table hierarchy resolves to more than one
            # top-level bucket, and every one of them is a real folder, not a leaf.
            assert len(top) > 1
            assert all(n.is_leaf is False for n in top)
            # At least one top-level folder has a real nested subfolder
            # of its own -- proving the hierarchy is genuinely recursive,
            # not just a wider flat list.
            has_nested_subfolder = False
            for folder in top:
                if any(child.is_leaf is False for child in await provider.children(folder)):
                    has_nested_subfolder = True
                    break
            assert has_nested_subfolder
        finally:
            await provider.close()


__all__: list[str] = []
