"""Tests for the vSphere side of examples/hypervisor-restore/restore_to_vmware_vsphere.py: ``VsphereTarget`` over a
fake ``VsphereSdk``, ``import_vm``'s lease handling with ``export_vmdk`` replaced, and ``restore``."""

from __future__ import annotations

import argparse
import asyncio
import threading
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from support.fakes import unchecked_fake
from unit.examples.hypervisor_restore_fakes import GIB, FakeRepo, make_plan, patch_session, vm_tree


class _Datacenter:
    def __init__(self) -> None:
        self.vmFolder = "vm-folder"


class _HostSystem:
    def __init__(self, name: str, parent: Any) -> None:
        self.name = name
        self.parent = parent
        self.datastore = [SimpleNamespace(name="ds1"), SimpleNamespace(name="ds2")]
        self.network = [SimpleNamespace(name="VM Network")]


class _Vim:
    """The ``vim`` types the target uses."""

    HostSystem = _HostSystem
    Datacenter = _Datacenter

    class OvfManager:
        NetworkMapping = SimpleNamespace
        CreateImportSpecParams = SimpleNamespace


@unchecked_fake("pyVmomi's HttpNfcLease")
class _Lease:
    def __init__(self, device_urls: list[tuple[str, str]], *, states: list[str] | None = None) -> None:
        self._states = states or ["initializing", "ready"]
        self.error: Any = None
        self.info: Any = SimpleNamespace(
            deviceUrl=[SimpleNamespace(importKey=key, url=url) for key, url in device_urls], entity="vm-1"
        )
        self.calls: list[tuple[Any, ...]] = []
        self.progressed = threading.Event()

    @property
    def state(self) -> str:
        return self._states.pop(0) if len(self._states) > 1 else self._states[0]

    def HttpNfcLeaseProgress(self, percent: int) -> None:
        self.calls.append(("progress", percent))
        self.progressed.set()

    def HttpNfcLeaseComplete(self) -> None:
        self.calls.append(("complete",))
        self.info = None
        self._states = ["done"]

    def HttpNfcLeaseAbort(self) -> None:
        self.calls.append(("abort",))
        self._states = ["error"]


@unchecked_fake("examples/hypervisor-restore/restore_to_vmware_vsphere.py's VsphereSdk, loaded at test time")
class _FakeSdk:
    """A vCenter (``api_type``) or ESXi host with the given hosts, and an import that answers with ``lease``."""

    def __init__(self, hosts: list[str], *, api_type: str = "VirtualCenter", lease: _Lease | None = None) -> None:
        self.vim = _Vim
        datacenter = _Datacenter()
        cluster = SimpleNamespace(parent=SimpleNamespace(parent=datacenter), resourcePool=self)
        self.hosts = [_HostSystem(name, cluster) for name in hosts]
        self.lease = lease or _Lease([("key-0", "https://*/nfc/disk-0.vmdk")])
        self.spec_errors: list[Any] = []
        self.file_items = [SimpleNamespace(path="disk0.vmdk", deviceId="key-0", create=False)]
        self.calls: list[tuple[Any, ...]] = []
        self.cookie = 'vmware_soap_session="s"'
        view = SimpleNamespace(view=self.hosts, Destroy=lambda: None)
        self.content = SimpleNamespace(
            about=SimpleNamespace(apiType=api_type),
            rootFolder="root",
            viewManager=SimpleNamespace(CreateContainerView=lambda *a: view),
            ovfManager=SimpleNamespace(CreateImportSpec=self._create_import_spec),
        )
        self.si = SimpleNamespace(RetrieveContent=lambda: self.content)

    def _create_import_spec(self, descriptor: str, pool: Any, datastore: Any, params: Any) -> Any:
        self.calls.append(("spec", descriptor, datastore.name, params))
        return SimpleNamespace(importSpec="spec", error=self.spec_errors, fileItem=self.file_items)

    def ImportVApp(self, spec: str, folder: str, host: Any) -> _Lease:
        self.calls.append(("import", spec, folder, host.name))
        return self.lease

    def disconnect(self) -> None:
        self.calls.append(("disconnect",))


def _args(**overrides: Any) -> argparse.Namespace:
    values: dict[str, Any] = {
        "vm_name": "apm-test-vm",
        "server": "esxi.example.com",
        "user": "root",
        "datastore": "ds1",
        "network": "VM Network",
        "cpus": 2,
        "memory_gib": 4,
        "disk_bus": "sata",
        "guest_id": "otherGuest64",
        "insecure": True,
        "upload": "stream",
        "power_on": False,
    }
    return argparse.Namespace(**(values | overrides))


@pytest.fixture(autouse=True)
def _no_waiting(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    real = ex.wait_until

    async def wait_until(probe: Any, *, what: str, limit: float | None) -> Any:
        return await real(probe, what=what, limit=limit, interval=0)

    monkeypatch.setattr(ex, "wait_until", wait_until)


# -- VsphereTarget ---------------------------------------------------------------


def test_target_takes_the_only_host_of_an_esxi_server(ex: ModuleType) -> None:
    target = ex.VsphereTarget(_FakeSdk(["esxi-1"], api_type="HostAgent"), None)

    assert target.host.name == "esxi-1" and isinstance(target.datacenter, _Datacenter)


def test_target_picks_the_host_named_under_vcenter(ex: ModuleType) -> None:
    assert ex.VsphereTarget(_FakeSdk(["esxi-1", "esxi-2"]), "esxi-2").host.name == "esxi-2"


@pytest.mark.parametrize(("host", "found"), [(None, "esxi-1, esxi-2"), ("esxi-9", "none")])
def test_target_needs_one_host(ex: ModuleType, host: str | None, found: str) -> None:
    with pytest.raises(ex.RestoreError, match=f"pass --host to pick one ESXi host \\(found: {found}\\)"):
        ex.VsphereTarget(_FakeSdk(["esxi-1", "esxi-2"]), host)


def test_target_finds_the_hosts_datastore_and_network(ex: ModuleType) -> None:
    target = ex.VsphereTarget(_FakeSdk(["esxi-1"]), None)

    assert target.datastore("ds2").name == "ds2" and target.network("VM Network").name == "VM Network"
    with pytest.raises(ex.RestoreError, match="host esxi-1 has no datastore 'ds9'"):
        target.datastore("ds9")
    with pytest.raises(ex.RestoreError, match="host esxi-1 has no network 'lan'"):
        target.network("lan")


def test_start_import_imports_thin_disks_on_the_host(ex: ModuleType) -> None:
    sdk = _FakeSdk(["esxi-1"])
    target = ex.VsphereTarget(sdk, None)

    lease, items = target.start_import("<ovf/>", "vm", target.datastore("ds1"), target.network("VM Network"))

    (_, descriptor, datastore, params), imported = sdk.calls
    assert (descriptor, datastore, params.entityName, params.diskProvisioning) == ("<ovf/>", "ds1", "vm", "thin")
    assert [(m.name, m.network.name) for m in params.networkMapping] == [("net0", "VM Network")]
    assert imported == ("import", "spec", "vm-folder", "esxi-1")
    assert lease is sdk.lease and items == sdk.file_items


def test_start_import_without_a_network_maps_none(ex: ModuleType) -> None:
    sdk = _FakeSdk(["esxi-1"])
    target = ex.VsphereTarget(sdk, None)

    target.start_import("<ovf/>", "vm", target.datastore("ds1"), None)

    assert sdk.calls[0][3].networkMapping == []


def test_start_import_reports_a_refused_spec(ex: ModuleType) -> None:
    sdk = _FakeSdk(["esxi-1"])
    sdk.spec_errors = [SimpleNamespace(localizedMessage="no space"), SimpleNamespace(localizedMessage="")]
    target = ex.VsphereTarget(sdk, None)

    with pytest.raises(ex.RestoreError, match="the import spec was refused: no space; "):
        target.start_import("<ovf/>", "vm", target.datastore("ds1"), None)
    assert [c[0] for c in sdk.calls] == ["spec"]


def test_lease_state_waits_out_initializing_and_reports_an_error(ex: ModuleType) -> None:
    lease = _Lease([], states=["initializing", "ready"])

    assert ex.VsphereTarget.lease_state(lease) is None
    assert ex.VsphereTarget.lease_state(lease) == "ready"
    lease._states, lease.error = ["error"], SimpleNamespace(msg="disk full")
    with pytest.raises(ex.RestoreError, match="the import failed: disk full"):
        ex.VsphereTarget.lease_state(lease)


def test_abort_leaves_a_finished_lease_alone(ex: ModuleType) -> None:
    done = _Lease([], states=["done"])
    ready = _Lease([], states=["ready"])

    ex.VsphereTarget.abort(done)
    ex.VsphereTarget.abort(ready)

    assert (done.calls, ready.calls) == ([], [("abort",)])


def test_task_done_follows_the_task(ex: ModuleType) -> None:
    def task(state: str) -> Any:
        return SimpleNamespace(info=SimpleNamespace(state=state, error=SimpleNamespace(msg="no license")))

    assert ex.VsphereTarget.task_done(task("running"), "powering on") is None
    assert ex.VsphereTarget.task_done(task("success"), "powering on") is True
    with pytest.raises(ex.RestoreError, match="powering on failed: no license"):
        ex.VsphereTarget.task_done(task("error"), "powering on")


# -- import_vm -------------------------------------------------------------------


@unchecked_fake("examples/hypervisor-restore/stream_vmdk.py's export_vmdk")
class _Exports:
    """Records each upload ``export_vmdk`` would open, failing at disk ``fail_at``."""

    def __init__(self, *, fail_at: int | None = None) -> None:
        self.uploads: list[dict[str, Any]] = []
        self._fail_at = fail_at

    async def __call__(self, disk: Any, label: str, open_upload: Any, *, two_pass: bool) -> None:
        if len(self.uploads) == self._fail_at:
            raise RuntimeError(f"{label} failed")
        self.uploads.append({"label": label, "url": open_upload.args[0], "two_pass": two_pass, **open_upload.keywords})


async def test_import_vm_uploads_each_disk_and_completes_the_lease(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    lease = _Lease([("key-0", "https://*/nfc/disk-0.vmdk"), ("key-1", "https://esxi-1/nfc/disk-1.vmdk")])
    sdk = _FakeSdk(["esxi-1"], lease=lease)
    sdk.file_items.append(SimpleNamespace(path="disk1.vmdk", deviceId="key-1", create=True))
    exports = _Exports()
    monkeypatch.setattr(ex, "export_vmdk", exports)

    vm = await ex.import_vm(_args(upload="two-pass"), make_plan(ex, GIB, 2 * GIB), ex.VsphereTarget(sdk, None), "c=1")

    assert vm == "vm-1"
    assert [(u["label"], u["url"], u["method"]) for u in exports.uploads] == [
        ("disk 1/2", "https://esxi.example.com/nfc/disk-0.vmdk", "POST"),
        ("disk 2/2", "https://esxi-1/nfc/disk-1.vmdk", "PUT"),
    ]
    assert all(u["two_pass"] for u in exports.uploads)
    assert exports.uploads[0]["headers"] == {"Cookie": "c=1", "Content-Type": "application/x-vnd.vmware-streamVmdk"}
    assert lease.calls == [("complete",)]


async def test_import_vm_aborts_the_lease_when_an_upload_fails(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    sdk = _FakeSdk(["esxi-1"])
    monkeypatch.setattr(ex, "export_vmdk", _Exports(fail_at=0))

    with pytest.raises(RuntimeError, match="disk 1/1 failed"):
        await ex.import_vm(_args(), make_plan(ex, GIB), ex.VsphereTarget(sdk, None), "c=1")

    assert sdk.lease.calls == [("abort",)]


async def test_import_vm_aborts_when_a_disk_has_no_upload_url(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    sdk = _FakeSdk(["esxi-1"], lease=_Lease([("other-key", "https://*/x")]))
    monkeypatch.setattr(ex, "export_vmdk", _Exports())

    with pytest.raises(ex.RestoreError, match="the import offers no upload URL for disk 1"):
        await ex.import_vm(_args(network=None), make_plan(ex, GIB), ex.VsphereTarget(sdk, None), "c=1")

    assert sdk.lease.calls == [("abort",)]


async def test_import_vm_keeps_the_lease_alive_during_a_long_upload(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    sdk = _FakeSdk(["esxi-1"])
    monkeypatch.setattr(ex, "_KEEPALIVE_SECONDS", 0)

    async def slow_export(*args: Any, two_pass: bool) -> None:
        assert await asyncio.to_thread(sdk.lease.progressed.wait, 10)

    monkeypatch.setattr(ex, "export_vmdk", slow_export)

    await ex.import_vm(_args(), make_plan(ex, GIB), ex.VsphereTarget(sdk, None), "c=1")

    assert sdk.lease.calls[-1] == ("complete",)


# -- restore ---------------------------------------------------------------------


async def test_restore_powers_the_vm_on_and_disconnects(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    patch_session(ex, monkeypatch, FakeRepo(*vm_tree()))
    sdk = _FakeSdk(["esxi-1"])
    monkeypatch.setattr(ex, "VsphereSdk", lambda server, user, password, *, insecure: sdk)
    monkeypatch.setattr(ex, "export_vmdk", _Exports())
    powered: list[str] = []

    def power_on(vm: str) -> Any:
        powered.append(vm)
        return SimpleNamespace(info=SimpleNamespace(state="success"))

    monkeypatch.setattr(ex.VsphereTarget, "power_on", staticmethod(power_on))
    args = _args(ref="repo#a/b/c", profile=None, key=None, firmware="bios", dry_run=False, host=None, power_on=True)

    await ex.restore(args, "pw")

    assert powered == ["vm-1"] and sdk.calls[-1] == ("disconnect",)
    assert "restored 1 disk(s) into VM apm-test-vm on ds1" in capsys.readouterr().out


async def test_restore_disconnects_after_a_failed_import(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    patch_session(ex, monkeypatch, FakeRepo(*vm_tree()))
    sdk = _FakeSdk(["esxi-1"])
    monkeypatch.setattr(ex, "VsphereSdk", lambda server, user, password, *, insecure: sdk)
    monkeypatch.setattr(ex, "export_vmdk", _Exports(fail_at=0))
    args = _args(ref="repo#a/b/c", profile=None, key=None, firmware="bios", dry_run=False, host=None)

    with pytest.raises(RuntimeError, match="failed"):
        await ex.restore(args, "pw")

    assert sdk.calls[-1] == ("disconnect",)
