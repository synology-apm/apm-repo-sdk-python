"""Regression test for ``synology_apm_repo.sdk.units.device``'s PC/PS
path, replayed from committed fixtures recorded against real, metadata-only
object-store samples, with **no external dependency**: these always run,
on CI or anywhere else, because they go through ``ReplayStore`` instead of
a real ``LocalFsStore``.

The fixtures (``tests/fixtures/``, recorded against a real store via
``Session.open_remote()`` -- see ``tests/CLAUDE.md``'s "Recording a
fixture" section for the ``pytest --record-against=...``/``make
record-fixture`` workflow that (re-)records these):

- ``device_pcps_ps_sample_1.json.gz`` -- ``ps-sample-1``, whose
  ``copy_target_file``-registered fids are all absent from ``file_meta``'s
  current generation.
- ``device_pcps_ps_sample_2.json.gz`` -- ``ps-sample-2``'s real PC
  (macOS, single disk) and PS (Windows, two disks/13 partitions) device
  workloads, both healthy and currently resolvable.

See ``tests/CLAUDE.md`` for what each sample covers; both were synced from
real S3/MinIO buckets with only ``@data/`` excluded, so neither fixture
records any bulk chunk content, only listing/catalog metadata.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.api import Session
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.device_kind import _NodeKind

#: Internal catalog identifiers -- stable and non-identifying (never
#: touched by catalog-metadata anonymization). Each real sample assigns
#: its own ids independently.
_PS_SAMPLE_1_WORKLOAD_ID = 2
_PS_SAMPLE_2_WORKLOAD_ID = 16


async def test_replayed_ps_sample_1_unresolvable_version_is_listed_and_opens_to_a_diagnostic(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    """``Catalog.versions()`` is the raw catalog read now — this version
    (whose registered fids are all absent from ``file_meta``'s current
    generation) is no longer excluded from the list; opening it still
    degrades gracefully to an empty, diagnostic-only disk list rather
    than raising, the same as it already did for any version reached
    through an unfiltered path (``verify_reachable()``'s own walk)."""
    store = await record_target("device_pcps_ps_sample_1.json.gz")
    async with Session() as session:
        [repo] = await session.open_remote(store)
        assert repo.is_encrypted is False

        [catalog] = await repo.catalogs()
        all_workloads = await catalog.workloads()
        ps = next(w for w in all_workloads if w.workload_id == _PS_SAMPLE_1_WORKLOAD_ID)
        [version] = await catalog.versions(ps)
        assert version.target_type == "PS"

        provider = await catalog.provider(version)
        assert provider.root().attrs["_kind"] == _NodeKind.PCPS_ROOT
        children = await provider.children(provider.root())
        assert not any(n.kind is UnitKind.DISK_IMAGE for n in children)
        assert any(n.attrs["_kind"] == _NodeKind.PCPS_DIAGNOSTIC for n in children)


async def test_replayed_ps_sample_2_pc_and_ps_are_both_healthy_and_listable(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("device_pcps_ps_sample_2.json.gz")
    async with Session() as session:
        [repo] = await session.open_remote(store)
        assert repo.is_encrypted is False

        [catalog] = await repo.catalogs()
        all_workloads = await catalog.workloads()

        pc = next(w for w in all_workloads if w.workload_type == "PC")
        [pc_version] = await catalog.versions(pc)
        pc_provider = await catalog.provider(pc_version)
        assert pc_provider.root().attrs["_kind"] == _NodeKind.PCPS_ROOT
        # Filtered to DISK_IMAGE: a dedup disk also gets an additive
        # "(filesystem)" sibling node (units/content/disk_fs.py, DISK_FILESYSTEM
        # kind) whenever pytsk3 is installed -- this test is about
        # disk-grouping fidelity, not that sibling.
        pc_children = await pc_provider.children(pc_provider.root())
        pc_disks = [n for n in pc_children if n.kind is UnitKind.DISK_IMAGE]
        assert len(pc_disks) == 1  # one un-fragmented macOS whole-disk object -> one singleton disk
        assert pc_disks[0].attrs["disk_uuid"] == "_single"
        assert all(child.attrs["_kind"] != _NodeKind.PCPS_DIAGNOSTIC for child in pc_children)

        ps = next(w for w in all_workloads if w.workload_id == _PS_SAMPLE_2_WORKLOAD_ID)
        ps_versions = await catalog.versions(ps)
        assert len(ps_versions) == 2  # a version Copied 2026-08-12, another 2026-08-18 — see tests/CLAUDE.md
        for version in ps_versions:
            provider = await catalog.provider(version)
            assert provider.root().attrs["_kind"] == _NodeKind.PCPS_ROOT
            children = await provider.children(provider.root())
            disks = [n for n in children if n.kind is UnitKind.DISK_IMAGE]
            assert len(disks) == 2  # 13 fragment objects grouped back down to 2 real physical disks
            assert all(d.attrs["_kind"] != _NodeKind.PCPS_DIAGNOSTIC for d in children)
            fragment_counts = sorted(len(d.attrs["fragments"]) for d in disks)
            assert fragment_counts == [4, 9]  # one 4-fragment disk, one 9-fragment disk


__all__: list[str] = []
