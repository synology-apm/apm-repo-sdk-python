"""Regression tests for ``synology_apm_repo.sdk.units.device``'s PC/PS path.
Each fixture's one test is its recording recipe:

- ``device_pcps_missing_fids_pc_encrypted.json.gz`` -- recorded against the
  ``pcps-encrypted`` sample, whose PC version's
  ``copy_target_file``-registered fids are all absent from ``file_meta``'s
  current generation.
- ``device_pcps_pc_and_ps.json.gz`` -- recorded against the
  ``pcps`` sample: a PC workload (one disk) and a PS workload (two
  disks, 13 fragments), both healthy.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from support.recording.sample_constants import PCPS_ENCRYPTED_KEY_STRING
from synology_apm_repo.sdk.api import Session
from synology_apm_repo.sdk.identifiers import CatalogId
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.units.base import UnitKind
from synology_apm_repo.sdk.units.device_handles import PcpsDiagnostic, PcpsDisk, PcpsRoot

#: The catalog holding each sample's PC/PS workloads; each bucket holds a
#: second catalog without them.
_PCPS_CATALOG_ID = CatalogId("jkwXlvh40ECN")
_PCPS_ENCRYPTED_CATALOG_ID = CatalogId("QH7cAZBYR8KN")
#: The ``pcps`` sample's PS workload id.
_PCPS_PS_WORKLOAD_ID = 2
#: The ``pcps-encrypted`` sample's PC workload id.
_PCPS_ENCRYPTED_PC_WORKLOAD_ID = 3


async def test_replayed_unresolvable_version_is_listed_and_opens_to_a_diagnostic(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    """A version whose registered fids are all absent from ``file_meta``'s
    current generation is still listed by ``Catalog.versions()``; opening it
    yields an empty, diagnostic-only disk list rather than raising."""
    store = await record_target("device_pcps_missing_fids_pc_encrypted.json.gz")
    async with Session() as session:
        [repo] = await session.open(store, PCPS_ENCRYPTED_KEY_STRING)
        assert repo.is_encrypted is True

        catalog = await repo.catalog_by_id(_PCPS_ENCRYPTED_CATALOG_ID)
        assert catalog is not None
        all_workloads = await catalog.workloads()
        pc = next(w for w in all_workloads if w.workload_id == _PCPS_ENCRYPTED_PC_WORKLOAD_ID)
        [version] = await catalog.versions(pc)
        assert version.target_type == "PC"

        provider = await catalog.provider(version)
        assert provider.root().handle == PcpsRoot()
        children = await provider.children(provider.root())
        assert not any(n.kind is UnitKind.DISK_IMAGE for n in children)
        assert any(isinstance(n.handle, PcpsDiagnostic) for n in children)


async def test_replayed_pc_and_ps_are_both_healthy_and_listable(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("device_pcps_pc_and_ps.json.gz")
    async with Session() as session:
        [repo] = await session.open(store)
        assert repo.is_encrypted is False

        catalog = await repo.catalog_by_id(_PCPS_CATALOG_ID)
        assert catalog is not None
        all_workloads = await catalog.workloads()

        pc = next(w for w in all_workloads if w.workload_type == "PC")
        [pc_version] = await catalog.versions(pc)
        pc_provider = await catalog.provider(pc_version)
        assert pc_provider.root().handle == PcpsRoot()
        # A disk may also get a DISK_FILESYSTEM sibling; count only DISK_IMAGE.
        pc_children = await pc_provider.children(pc_provider.root())
        pc_disks = [n for n in pc_children if n.kind is UnitKind.DISK_IMAGE]
        assert len(pc_disks) == 1  # one un-fragmented whole-disk object -> one singleton disk
        assert pc_disks[0].details["disk_uuid"] == "_single"
        assert all(not isinstance(child.handle, PcpsDiagnostic) for child in pc_children)

        ps = next(w for w in all_workloads if w.workload_id == _PCPS_PS_WORKLOAD_ID)
        [ps_version] = await catalog.versions(ps)
        provider = await catalog.provider(ps_version)
        assert provider.root().handle == PcpsRoot()
        children = await provider.children(provider.root())
        disks = [n for n in children if n.kind is UnitKind.DISK_IMAGE]
        assert len(disks) == 2  # 13 fragment objects grouped back down to 2 real physical disks
        assert all(not isinstance(d.handle, PcpsDiagnostic) for d in children)
        fragment_counts = sorted(len(d.handle.fragments) for d in disks if isinstance(d.handle, PcpsDisk))
        assert fragment_counts == [4, 9]
