"""Tests for the Proxmox VE side of examples/hypervisor-restore/restore_to_proxmox_ve.py: ``vm_config``,
``multipart_framing``, ``PveApi``'s requests, ``PveTarget`` and ``execute_restore`` over a fake ``PveApi``, with
``export_vmdk`` replaced."""

from __future__ import annotations

import argparse
import io
import json
import ssl
import urllib.error
import urllib.parse
from collections.abc import Callable
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar

import pytest

from support.fakes import unchecked_fake
from unit.examples.hypervisor_restore_fakes import GIB, make_plan

_NODE = "pve1"


def _args(**overrides: Any) -> argparse.Namespace:
    values: dict[str, Any] = {
        "vm_name": "apm-test-vm",
        "cpus": 2,
        "memory_gib": 4,
        "cpu_type": "x86-64-v2-AES",
        "ostype": "l26",
        "disk_bus": "sata",
        "target_storage": "local-lvm",
        "efi_storage": None,
        "bridge": "vmbr0",
        "staging_dir": "/var/tmp/",
        "name_prefix": "apm-restore",
        "upload": "two-pass",
    }
    return argparse.Namespace(**(values | overrides))


@pytest.fixture(autouse=True)
def _no_waiting(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    real = ex.wait_until

    async def wait_until(probe: Any, *, what: str, limit: float | None) -> Any:
        return await real(probe, what=what, limit=limit, interval=0)

    monkeypatch.setattr(ex, "wait_until", wait_until)


# -- vm_config and multipart_framing ---------------------------------------------


def test_vm_config_imports_each_disk_on_sata(ex: ModuleType) -> None:
    volumes = ["st:import/a-0.vmdk", "st:import/a-1.vmdk"]

    config = ex.vm_config(_args(), make_plan(ex, GIB, GIB, firmware="bios"), volumes, vmid=120)

    assert config == {
        "vmid": 120,
        "name": "apm-test-vm",
        "cores": 2,
        "memory": 4096,
        "cpu": "x86-64-v2-AES",
        "ostype": "l26",
        "machine": "q35",
        "boot": "order=sata0",
        "sata0": "local-lvm:0,import-from=st:import/a-0.vmdk",
        "sata1": "local-lvm:0,import-from=st:import/a-1.vmdk",
        "net0": "virtio,bridge=vmbr0",
    }


def test_vm_config_for_uefi_on_scsi_without_a_nic(ex: ModuleType) -> None:
    args = _args(disk_bus="scsi", efi_storage="nvme", bridge=None)

    config = ex.vm_config(args, make_plan(ex, GIB), ["st:import/a-0.vmdk"], vmid=7)

    assert (config["boot"], config["scsihw"], config["scsi0"]) == (
        "order=scsi0",
        "virtio-scsi-single",
        "local-lvm:0,import-from=st:import/a-0.vmdk",
    )
    assert (config["bios"], config["efidisk0"]) == ("ovmf", "nvme:1,efitype=4m,pre-enrolled-keys=0")
    assert "net0" not in config


def test_vm_config_puts_the_efi_disk_on_the_target_storage_by_default(ex: ModuleType) -> None:
    config = ex.vm_config(_args(), make_plan(ex, GIB), ["v"], vmid=7)

    assert config["efidisk0"].startswith("local-lvm:1,")


def test_vm_config_refuses_more_disks_than_sata_ports(ex: ModuleType) -> None:
    with pytest.raises(ex.RestoreError, match="7 disks do not fit on the 6 SATA ports; use --disk-bus scsi"):
        ex.vm_config(_args(), make_plan(ex, *[GIB] * 7), ["v"] * 7, vmid=7)
    assert len(ex.vm_config(_args(disk_bus="scsi"), make_plan(ex, *[GIB] * 7), ["v"] * 7, vmid=7)) > 7


def test_multipart_framing_wraps_the_file_in_an_import_form(ex: ModuleType) -> None:
    prefix, suffix = ex.multipart_framing("a-0.vmdk", boundary="B")

    body = prefix + b"VMDK" + suffix
    assert body == (
        b'--B\r\nContent-Disposition: form-data; name="content"\r\n\r\nimport\r\n'
        b'--B\r\nContent-Disposition: form-data; name="filename"; filename="a-0.vmdk"\r\n'
        b"Content-Type: application/octet-stream\r\n\r\nVMDK\r\n--B--\r\n"
    )


def test_upload_volume_names_the_import_volume(ex: ModuleType) -> None:
    assert ex.upload_volume(b'{"data": "UPID:x"}', "st", "a-0.vmdk") == ("UPID:x", "st:import/a-0.vmdk")
    with pytest.raises(ex.RestoreError, match=r"the upload of a-0\.vmdk returned no task"):
        ex.upload_volume(b'{"data": null}', "st", "a-0.vmdk")


# -- PveApi ----------------------------------------------------------------------


class _Opener:
    """Stands in for ``urllib.request.urlopen``: records each request and answers with ``answer(request)``."""

    def __init__(self, answer: Callable[[Any], Any]) -> None:
        self.requests: list[Any] = []
        self._answer = answer

    def __call__(self, request: Any, *, context: ssl.SSLContext) -> io.BytesIO:
        self.requests.append(request)
        return io.BytesIO(json.dumps({"data": self._answer(request)}).encode())


def _login_answer(request: Any) -> Any:
    if request.full_url.endswith("/access/ticket"):
        return {"ticket": "T", "CSRFPreventionToken": "C"}
    return "UPID:task"


def test_api_logs_in_and_sends_the_ticket(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    opener = _Opener(_login_answer)
    monkeypatch.setattr(ex.urllib.request, "urlopen", opener)

    api = ex.PveApi("pve.example.com", "root@pam", "pw", context=ssl.create_default_context())
    answer = api.post("/nodes/pve1/qemu", vmid=7, name="a b")

    login, create = opener.requests
    assert login.full_url == "https://pve.example.com:8006/api2/json/access/ticket"
    assert urllib.parse.parse_qs(login.data.decode()) == {"username": ["root@pam"], "password": ["pw"]}
    assert "Cookie" not in login.headers
    assert (create.method, create.full_url) == ("POST", "https://pve.example.com:8006/api2/json/nodes/pve1/qemu")
    assert urllib.parse.parse_qs(create.data.decode()) == {"vmid": ["7"], "name": ["a b"]}
    assert (create.headers["Cookie"], create.headers["Csrfpreventiontoken"]) == ("PVEAuthCookie=T", "C")
    assert answer == "UPID:task"


@pytest.mark.parametrize("method", ["get", "delete"])
def test_api_sends_get_and_delete_parameters_in_the_query(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, method: str
) -> None:
    opener = _Opener(_login_answer)
    monkeypatch.setattr(ex.urllib.request, "urlopen", opener)
    api = ex.PveApi("pve", "root@pam", "pw", context=ssl.create_default_context())

    getattr(api, method)("/nodes/pve1/qemu/7", purge=1)

    request = opener.requests[-1]
    assert (request.method, request.data) == (method.upper(), None)
    assert request.full_url.endswith("/nodes/pve1/qemu/7?purge=1")


def test_api_reports_an_http_error_with_its_detail(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(request: Any, *, context: ssl.SSLContext) -> Any:
        raise urllib.error.HTTPError(request.full_url, 401, "authentication failure", {}, io.BytesIO(b" no "))  # type: ignore[arg-type]

    monkeypatch.setattr(ex.urllib.request, "urlopen", refuse)

    with pytest.raises(ex.RestoreError, match=r"^POST /access/ticket failed: HTTP 401 authentication failure no$"):
        ex.PveApi("pve", "root@pam", "bad", context=ssl.create_default_context())


# -- PveTarget -------------------------------------------------------------------


@unchecked_fake("examples/hypervisor-restore/restore_to_proxmox_ve.py's PveApi, loaded at test time")
class _FakeApi:
    """A node ``pve1`` with storages ``local``/``local-lvm`` and VMs ``vms``; every task ends ``OK`` unless
    named in ``failing`` (by the path whose call started it)."""

    base = "https://pve:8006/api2/json"
    context = ssl.create_default_context()
    auth_headers: ClassVar[dict[str, str]] = {"Cookie": "PVEAuthCookie=T", "CSRFPreventionToken": "C"}

    def __init__(self, ex: ModuleType) -> None:
        self._error = ex.RestoreError
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.vms: set[int] = set()
        self.failing: set[str] = set()
        self._tasks: dict[str, str] = {}

    def _task(self, path: str) -> str:
        upid = f"UPID:{len(self._tasks)}"
        self._tasks[upid] = "failed: no space" if path in self.failing else "OK"
        return upid

    def get(self, path: str, /, **params: Any) -> Any:
        self.calls.append(("get", path, params))
        if path.endswith("/status"):
            return {"status": "stopped", "exitstatus": self._tasks[path.split("/")[-2]]}
        if path == f"/nodes/{_NODE}/storage":
            return [{"storage": "local"}, {"storage": "local-lvm"}]
        if path == f"/nodes/{_NODE}/qemu":
            return [{"vmid": str(vmid)} for vmid in sorted(self.vms)]
        if path == "/cluster/nextid":
            return "105"
        raise AssertionError(path)

    def post(self, path: str, /, **params: Any) -> Any:
        self.calls.append(("post", path, params))
        if path == f"/nodes/{_NODE}/qemu":
            self.vms.add(int(params["vmid"]))
        return None if path == "/storage" else self._task(path)

    def delete(self, path: str, /, **params: Any) -> Any:
        self.calls.append(("delete", path, params))
        if path.startswith(f"/nodes/{_NODE}/qemu/"):
            vmid = int(path.rsplit("/", 1)[1])
            if vmid not in self.vms:
                raise self._error(f"DELETE {path} failed: HTTP 500 Configuration file '{vmid}.conf' does not exist")
            self.vms.discard(vmid)
        return None if path.startswith("/storage/") else self._task(path)


def _changes(api: _FakeApi) -> list[tuple[str, str]]:
    """The calls that change something, in order."""
    return [(method, path) for method, path, _ in api.calls if method != "get"]


async def test_check_storages_names_the_missing_ones(ex: ModuleType) -> None:
    pve = ex.PveTarget(_FakeApi(ex), _NODE)

    await pve.check_storages({"local-lvm"})
    with pytest.raises(ex.RestoreError, match="node pve1 has no storage ceph, nvme"):
        await pve.check_storages({"local", "nvme", "ceph"})


async def test_wait_task_reports_a_failed_task(ex: ModuleType) -> None:
    api = _FakeApi(ex)
    api.failing.add("/x")
    pve = ex.PveTarget(api, _NODE)

    with pytest.raises(ex.RestoreError, match="creating the VM failed: failed: no space"):
        await pve.wait_task(api.post("/x"), "creating the VM")


async def test_wait_task_waits_while_the_task_runs(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    api = _FakeApi(ex)
    answers = iter([{"status": "running"}, {"status": "stopped", "exitstatus": "OK"}])
    monkeypatch.setattr(api, "get", lambda path, /, **params: next(answers))

    await ex.PveTarget(api, _NODE).wait_task("UPID:0", "x")


async def test_add_import_storage_adds_a_dir_storage_for_imports_on_the_node(ex: ModuleType) -> None:
    api = _FakeApi(ex)

    await ex.PveTarget(api, _NODE).add_import_storage("apm-x", "/var/tmp/apm-x")

    assert api.calls == [
        (
            "post",
            "/storage",
            {"storage": "apm-x", "type": "dir", "path": "/var/tmp/apm-x", "content": "import", "nodes": _NODE},
        )
    ]


def test_upload_posts_a_multipart_form_to_the_storage(ex: ModuleType) -> None:
    upload = ex.PveTarget(_FakeApi(ex), _NODE).upload("apm-x", "apm-x-0.vmdk", content_length=10)

    assert upload._url.geturl() == "https://pve:8006/api2/json/nodes/pve1/storage/apm-x/upload"
    assert upload._headers["Cookie"] == "PVEAuthCookie=T"
    boundary = upload._headers["Content-Type"].removeprefix("multipart/form-data; boundary=")
    assert (upload._prefix, upload._suffix) == ex.multipart_framing("apm-x-0.vmdk", boundary=boundary)
    assert upload._content_length == 10


async def test_delete_volume_quotes_the_volume_id(ex: ModuleType) -> None:
    api = _FakeApi(ex)

    await ex.PveTarget(api, _NODE).delete_volume("apm-x", "apm-x:import/apm-x-0.vmdk")

    assert _changes(api) == [("delete", "/nodes/pve1/storage/apm-x/content/apm-x%3Aimport%2Fapm-x-0.vmdk")]


async def test_destroy_vm_goes_by_id_and_accepts_a_vm_already_gone(ex: ModuleType) -> None:
    """Destroys by id, without listing VMs first."""
    api = _FakeApi(ex)
    api.vms = {120}
    pve = ex.PveTarget(api, _NODE)

    await pve.destroy_vm(121)
    await pve.destroy_vm(120)

    assert [(m, p, params) for m, p, params in api.calls if m != "get" or "/tasks/" not in p] == [
        ("delete", "/nodes/pve1/qemu/121", {"purge": 1}),
        ("delete", "/nodes/pve1/qemu/120", {"purge": 1}),
    ]
    assert api.vms == set()


async def test_discard_vm_destroys_the_vm_only_once_its_create_task_ended(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = _FakeApi(ex)
    upid = api.post("/nodes/pve1/qemu", vmid=120)
    running = iter(range(2))
    real_get = api.get

    def get(path: str, /, **params: Any) -> Any:
        if path.endswith(f"/tasks/{upid}/status") and next(running, None) is not None:
            api.calls.append(("get", path, params))
            return {"status": "running"}
        return real_get(path, **params)

    monkeypatch.setattr(api, "get", get)

    await ex.PveTarget(api, _NODE).discard_vm(upid, 120)

    polls = [i for i, (m, p, _) in enumerate(api.calls) if p.endswith(f"/tasks/{upid}/status")]
    assert len(polls) == 3 and polls[-1] < api.calls.index(("delete", "/nodes/pve1/qemu/120", {"purge": 1}))
    assert api.vms == set()


# -- execute_restore -------------------------------------------------------------


@unchecked_fake("examples/hypervisor-restore/stream_vmdk.py's export_vmdk")
class _Exports:
    """Records each upload ``export_vmdk`` would make, answering with an upload task; fails at disk ``fail_at``."""

    def __init__(self, *, fail_at: int | None = None) -> None:
        self.uploads: list[tuple[str, str, bool]] = []
        self._fail_at = fail_at

    async def __call__(self, disk: Any, label: str, open_upload: Any, *, two_pass: bool, file_name: str) -> Any:
        if len(self.uploads) == self._fail_at:
            raise RuntimeError(f"{label} failed")
        storage, filename = open_upload.args
        assert filename == file_name
        self.uploads.append((storage, filename, two_pass))
        return SimpleNamespace(response=json.dumps({"data": f"UPID:upload-{len(self.uploads)}"}).encode())


@pytest.fixture
def api(ex: ModuleType, monkeypatch: pytest.MonkeyPatch) -> _FakeApi:
    fake = _FakeApi(ex)
    real_get = fake.get

    def get(path: str, /, **params: Any) -> Any:
        if "/tasks/UPID:upload-" in path:
            fake.calls.append(("get", path, params))
            return {"status": "stopped", "exitstatus": "OK"}
        return real_get(path, **params)

    monkeypatch.setattr(fake, "get", get)
    monkeypatch.setattr(ex, "restore_name", lambda prefix, vm_name: f"{prefix}-{vm_name}-abc123")
    return fake


async def test_execute_restore_uploads_creates_the_vm_and_cleans_up(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, api: _FakeApi
) -> None:
    exports = _Exports()
    monkeypatch.setattr(ex, "export_vmdk", exports)

    await ex.execute_restore(_args(), make_plan(ex, GIB, GIB), ex.PveTarget(api, _NODE), vmid=120)

    storage = "apm-restore-apm-test-vm-abc123"
    assert exports.uploads == [(storage, f"{storage}-0.vmdk", True), (storage, f"{storage}-1.vmdk", True)]
    assert _changes(api) == [
        ("post", "/storage"),
        ("post", "/nodes/pve1/qemu"),
        ("delete", f"/nodes/pve1/storage/{storage}/content/{storage}%3Aimport%2F{storage}-0.vmdk"),
        ("delete", f"/nodes/pve1/storage/{storage}/content/{storage}%3Aimport%2F{storage}-1.vmdk"),
        ("delete", f"/storage/{storage}"),
    ]
    assert api.calls[next(i for i, c in enumerate(api.calls) if c[1] == "/storage")][2]["path"] == (
        f"/var/tmp/{storage}"
    )
    create = next(params for method, path, params in api.calls if path == "/nodes/pve1/qemu" and method == "post")
    assert create["sata1"] == f"local-lvm:0,import-from={storage}:import/{storage}-1.vmdk"


async def test_execute_restore_removes_the_uploads_and_storage_when_an_upload_fails(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, api: _FakeApi
) -> None:
    monkeypatch.setattr(ex, "export_vmdk", _Exports(fail_at=1))

    with pytest.raises(RuntimeError, match="disk 2/2 failed"):
        await ex.execute_restore(_args(upload="stream"), make_plan(ex, GIB, GIB), ex.PveTarget(api, _NODE), vmid=120)

    storage = "apm-restore-apm-test-vm-abc123"
    assert _changes(api) == [
        ("post", "/storage"),
        ("delete", f"/nodes/pve1/storage/{storage}/content/{storage}%3Aimport%2F{storage}-0.vmdk"),
        ("delete", f"/storage/{storage}"),
    ]


async def test_execute_restore_destroys_a_vm_whose_import_failed(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, api: _FakeApi
) -> None:
    monkeypatch.setattr(ex, "export_vmdk", _Exports())
    api.failing.add("/nodes/pve1/qemu")

    with pytest.raises(ex.RestoreError, match="creating the VM \\(importing its disks\\) failed"):
        await ex.execute_restore(_args(), make_plan(ex, GIB), ex.PveTarget(api, _NODE), vmid=120)

    assert next(c for c in _changes(api) if c[0] == "delete") == ("delete", "/nodes/pve1/qemu/120")
    assert _changes(api)[-1] == ("delete", "/storage/apm-restore-apm-test-vm-abc123")


async def test_execute_restore_checks_the_storages_before_changing_anything(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, api: _FakeApi
) -> None:
    monkeypatch.setattr(ex, "export_vmdk", _Exports())

    with pytest.raises(ex.RestoreError, match="node pve1 has no storage nvme"):
        await ex.execute_restore(_args(efi_storage="nvme"), make_plan(ex, GIB), ex.PveTarget(api, _NODE), vmid=120)

    assert _changes(api) == []


async def test_execute_restore_only_warns_when_removing_the_uploads_fails(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, api: _FakeApi, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(ex, "export_vmdk", _Exports())
    storage = "apm-restore-apm-test-vm-abc123"
    api.failing.add(f"/nodes/pve1/storage/{storage}/content/{storage}%3Aimport%2F{storage}-0.vmdk")

    await ex.execute_restore(_args(), make_plan(ex, GIB), ex.PveTarget(api, _NODE), vmid=120)

    assert 120 in api.vms and ("delete", "/nodes/pve1/qemu/120") not in _changes(api)
    assert f"warning: VM 120 was restored, but storage {storage} and its uploads were left behind" in (
        capsys.readouterr().err
    )


async def test_execute_restore_leaves_an_existing_vm_whose_id_the_create_was_refused(
    ex: ModuleType, monkeypatch: pytest.MonkeyPatch, api: _FakeApi
) -> None:
    """``--vmid`` of a VM that exists: the create is refused, and nothing destroys that VM."""
    monkeypatch.setattr(ex, "export_vmdk", _Exports())
    api.vms = {120}

    def refuse(path: str, /, **params: Any) -> Any:
        if path == "/nodes/pve1/qemu":
            raise ex.RestoreError("POST /nodes/pve1/qemu failed: HTTP 500 VM 120 already exists")
        return _FakeApi.post(api, path, **params)

    monkeypatch.setattr(api, "post", refuse)

    with pytest.raises(ex.RestoreError, match="VM 120 already exists"):
        await ex.execute_restore(_args(), make_plan(ex, GIB), ex.PveTarget(api, _NODE), vmid=120)

    assert api.vms == {120}
    assert ("delete", "/nodes/pve1/qemu/120") not in _changes(api)
