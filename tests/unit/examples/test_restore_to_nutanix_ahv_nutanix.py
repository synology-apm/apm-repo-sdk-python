"""Tests for the Nutanix half of examples/restore_to_nutanix_ahv.py: ``AhvTarget``'s logic (task waiting,
Volume Group creation and its cleanup, hydration waiting, deletes of what this run created) over ``_FakeSdk``.

The ``NutanixSdk`` adapter itself (model construction against the real ``ntnx_*`` packages) is not
exercised here.

``_FakeSdk`` offers no way to look an object up by name, so any code path that tried would fail at once:
everything ``AhvTarget`` does must go by id.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from support.fakes import unchecked_fake

_VG_REL = "volumes:config:volume-group"
_VG_DISK_REL = "volumes:config:volume-group:disk"
_VM_REL = "vmm:ahv:config:vm"
_VM_DISK_REL = "vmm:ahv:config:vm:disk"


class _Cloud:
    """Shared state of the fake Prism Central: what exists, what was called, and what should fail."""

    def __init__(self) -> None:
        self.log: list[tuple[Any, ...]] = []
        self.groups: dict[str, dict[str, Any]] = {}
        self.vms: dict[str, dict[str, Any]] = {}
        self.fail_calls: dict[str, BaseException] = {}
        self.task_ops: dict[str, str] = {}
        self.task_scripts: dict[str, list[Any]] = {}
        self.task_created: dict[str, list[tuple[str, str]]] = {}
        self.error_messages: Any = None
        self.cancel_ends_task = True  # False: a cancel request changes nothing
        self.omit_entities = False  # True: finished create tasks carry no entity reference
        self.reported_index_offset = 0  # what a disk reports as its index differs from what was asked for
        self.reported_size_offset = 0  # ... and as its size
        self.report_no_index = False
        self.report_no_size = False
        self.hydration_script: list[Any] = [None]
        self.polls = 0
        self._counter = 0

    def next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}-{self._counter}"

    def task(self, operation: str, created: list[tuple[str, str]] | None = None) -> SimpleNamespace:
        task_id = self.next_id("task")
        self.task_ops[task_id] = operation
        if created is not None:
            self.task_created[task_id] = created
        return SimpleNamespace(data=SimpleNamespace(ext_id=task_id))

    def call(self, method: str, *args: Any) -> None:
        self.log.append((method, *args))
        if method in self.fail_calls:
            raise self.fail_calls[method]

    def names(self) -> list[str]:
        return [entry[0] for entry in self.log]


@unchecked_fake("the ntnx_* SDK's Tasks API")
class _FakeTasks:
    def __init__(self, cloud: _Cloud) -> None:
        self._cloud = cloud

    def get_task_by_id(self, task_id: str) -> SimpleNamespace:
        cloud = self._cloud
        cloud.polls += 1
        script = cloud.task_scripts.get(cloud.task_ops[task_id], ["SUCCEEDED"])
        status = script.pop(0) if len(script) > 1 else script[0]
        created = cloud.task_created.get(task_id)
        entities = (
            None
            if created is None or cloud.omit_entities
            else [SimpleNamespace(ext_id=ext_id, rel=rel) for ext_id, rel in created]
        )
        task = SimpleNamespace(
            ext_id=task_id,
            status=status,
            error_messages=cloud.error_messages,
            legacy_error_message="legacy boom",
            entities_affected=entities,
        )
        return SimpleNamespace(data=task)

    def cancel_task(self, task_id: str) -> None:
        cloud = self._cloud
        cloud.call("cancel_task", task_id)
        if cloud.cancel_ends_task:
            cloud.task_scripts[cloud.task_ops[task_id]] = ["CANCELED"]


@unchecked_fake("the ntnx_* SDK's VolumeGroups API")
class _FakeVolumeGroups:
    """Everything is by id; there is deliberately no list/lookup-by-name method."""

    def __init__(self, cloud: _Cloud) -> None:
        self._cloud = cloud

    def create_volume_group(self, body: Any) -> SimpleNamespace:
        self._cloud.call("create_volume_group", body.name)
        group_id = self._cloud.next_id("vg")
        self._cloud.groups[group_id] = {"name": body.name, "disks": [], "attachments": []}
        return self._cloud.task("create_volume_group", created=[(group_id, _VG_REL)])

    def create_volume_disk(self, group_id: str, body: Any) -> SimpleNamespace:
        self._cloud.call("create_volume_disk", group_id, body.index)
        disk_id = self._cloud.next_id("disk")
        self._cloud.groups[group_id]["disks"].append(SimpleNamespace(index=body.index, size=body.size, ext_id=disk_id))
        # A disk's create task names the group as well as the disk, as Prism Central does.
        return self._cloud.task("create_volume_disk", created=[(group_id, _VG_REL), (disk_id, _VG_DISK_REL)])

    def get_volume_disk_by_id(self, group_id: str, disk_id: str) -> SimpleNamespace:
        self._cloud.call("get_volume_disk_by_id", group_id, disk_id)
        # An unknown id must be an ordinary error: a StopIteration cannot be raised into an asyncio future.
        matches = [d for d in self._cloud.groups[group_id]["disks"] if d.ext_id == disk_id]
        if not matches:
            raise KeyError(f"no disk {disk_id} in {group_id}")
        disk = matches[0]
        index = None if self._cloud.report_no_index else disk.index + self._cloud.reported_index_offset
        size = None if self._cloud.report_no_size else disk.size + self._cloud.reported_size_offset
        return SimpleNamespace(data=SimpleNamespace(ext_id=disk_id, index=index, disk_size_bytes=size))

    def get_volume_group_by_id(self, group_id: str) -> SimpleNamespace:
        self._cloud.call("get_volume_group_by_id", group_id)
        group = self._cloud.groups[group_id]
        return SimpleNamespace(
            data=SimpleNamespace(ext_id=group_id, target_name=f"target-{group_id}", name=group["name"])
        )

    def attach_iscsi_client(self, group_id: str, body: Any) -> SimpleNamespace:
        self._cloud.call("attach_iscsi_client", group_id, body.iqn)
        client = SimpleNamespace(ext_id=self._cloud.next_id("client"), iqn=body.iqn)
        self._cloud.groups[group_id]["attachments"].append(client)
        return self._cloud.task("attach_iscsi_client")

    def list_external_iscsi_attachments_by_volume_group_id(self, group_id: str) -> SimpleNamespace:
        self._cloud.call("list_external_iscsi_attachments_by_volume_group_id", group_id)
        return SimpleNamespace(data=list(self._cloud.groups[group_id]["attachments"]))

    def detach_iscsi_client(self, group_id: str, body: Any) -> SimpleNamespace:
        self._cloud.call("detach_iscsi_client", group_id, body.ext_id)
        group = self._cloud.groups[group_id]
        group["attachments"] = [c for c in group["attachments"] if c.ext_id != body.ext_id]
        return self._cloud.task("detach_iscsi_client")

    def delete_volume_group_by_id(self, group_id: str) -> SimpleNamespace:
        self._cloud.call("delete_volume_group_by_id", group_id)
        if self._cloud.groups[group_id]["attachments"]:
            raise RuntimeError("VG must be detached before it can be deleted")
        del self._cloud.groups[group_id]
        return self._cloud.task("delete_volume_group")


@unchecked_fake("the ntnx_* SDK's Vms API")
class _FakeVms:
    def __init__(self, cloud: _Cloud) -> None:
        self._cloud = cloud

    def create_vm(self, body: Any) -> SimpleNamespace:
        self._cloud.call("create_vm", body.name)
        vm_id = self._cloud.next_id("vm")
        self._cloud.vms[vm_id] = {"name": body.name}
        disks = [(self._cloud.next_id("vmdisk"), _VM_DISK_REL) for _ in body.sizes]
        return self._cloud.task("create_vm", created=[(vm_id, _VM_REL), *disks])

    def list_disks_by_vm_id(self, vm_id: str) -> SimpleNamespace:
        self._cloud.call("list_disks_by_vm_id", vm_id)
        script = self._cloud.hydration_script
        state = script.pop(0) if len(script) > 1 else script[0]
        info = None if state is None else SimpleNamespace(disk_hydration_status=state)
        return SimpleNamespace(data=[SimpleNamespace(backing_info=SimpleNamespace(vm_disk_hydration_info=info))])

    def get_vm_by_id(self, vm_id: str) -> SimpleNamespace:
        self._cloud.call("get_vm_by_id", vm_id)
        return SimpleNamespace(data=SimpleNamespace(name=self._cloud.vms[vm_id]["name"]))

    def delete_vm_by_id(self, vm_id: str) -> SimpleNamespace:
        self._cloud.call("delete_vm_by_id", vm_id)
        del self._cloud.vms[vm_id]
        return self._cloud.task("delete_vm")


@unchecked_fake("the ntnx_* SDK's Client API")
class _FakeClient:
    def __init__(self, cloud: _Cloud, kind: str) -> None:
        self._cloud, self._kind = cloud, kind

    def add_default_header(self, header_name: str, header_value: str) -> None:
        self._cloud.log.append(("header", self._kind, header_name, header_value))

    def get_etag(self, response: Any) -> str:
        return f"etag-{response.data.name}"


@unchecked_fake("the ntnx_* SDK's Sdk API")
class _FakeSdk:
    """The ``NutanixSdk`` surface ``AhvTarget`` uses, over ``_Cloud``; models are plain namespaces."""

    def __init__(self, cloud: _Cloud) -> None:
        self.cloud = cloud
        self.volume_groups = _FakeVolumeGroups(cloud)
        self.vms = _FakeVms(cloud)
        self.tasks = _FakeTasks(cloud)

    def scoped(self, kind: str) -> tuple[_FakeClient, Any]:
        return _FakeClient(self.cloud, kind), (self.volume_groups if kind == "volume_group" else self.vms)

    def volume_group(self, name: str, cluster: str) -> SimpleNamespace:
        return SimpleNamespace(name=name, cluster=cluster)

    def volume_disk(self, index: int, size_bytes: int, container: str) -> SimpleNamespace:
        return SimpleNamespace(index=index, size=size_bytes, container=container)

    def iscsi_client(self, iqn: str) -> SimpleNamespace:
        return SimpleNamespace(iqn=iqn)

    def iscsi_attachment(self, ext_id: str) -> SimpleNamespace:
        return SimpleNamespace(ext_id=ext_id)

    def vm(
        self, name: str, cluster: str, container: str, group: Any, sizes: list[int], **options: Any
    ) -> SimpleNamespace:
        return SimpleNamespace(name=name, cluster=cluster, container=container, group=group, sizes=sizes, **options)


@pytest.fixture
def cloud() -> _Cloud:
    return _Cloud()


class _Clock:
    """A fake clock: ``sleep`` advances it instantly, so waits are exact and take no real time."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
        await asyncio.sleep(0)


@pytest.fixture
def clock() -> _Clock:
    return _Clock()


@pytest.fixture
def target_factory(ex: ModuleType, cloud: _Cloud, clock: _Clock) -> Callable[..., Any]:
    def make(**kwargs: Any) -> Any:
        options: dict[str, Any] = {
            "poll_seconds": 1.0,
            "settle_seconds": 10.0,
            "clock": clock.monotonic,
            "sleep": clock.sleep,
        }
        return ex.AhvTarget(_FakeSdk(cloud), **{**options, **kwargs})

    return make


_GROUP = SimpleNamespace(ext_id="vg", disk_ext_ids=("d",), disks=(), target_name="t")


def _list_instead(
    cloud: _Cloud, operation: str, rewrite: Callable[[list[tuple[str, str]]], list[tuple[str, str]]]
) -> None:
    """Makes ``operation``'s tasks list ``rewrite(the (id, rel) entities the fake made)`` instead."""
    real_task = cloud.task

    def task(op: str, created: list[tuple[str, str]] | None = None) -> SimpleNamespace:
        response = real_task(op, created)
        if op == operation and created is not None:
            cloud.task_created[response.data.ext_id] = rewrite(list(created))
        return response

    cloud.task = task  # type: ignore[method-assign,assignment]


async def _create_vm(target: Any, name: str = "vm") -> str:
    vm_id: str = await target.create_vm(name, "c", "k", _GROUP, [10], cpus=1, memory_bytes=2, uefi=False)
    return vm_id


# -- _status_name / task waiting -------------------------------------------------


@pytest.mark.parametrize(
    ("status", "expected"),
    [("SUCCEEDED", "SUCCEEDED"), ("TaskStatus.FAILED", "FAILED"), (SimpleNamespace(name="RUNNING"), "RUNNING")],
)
def test_status_name_accepts_strings_and_enum_likes(ex: ModuleType, status: object, expected: str) -> None:
    assert ex._status_name(status) == expected


async def test_wait_polls_until_the_task_succeeds(
    cloud: _Cloud, clock: _Clock, target_factory: Callable[..., Any]
) -> None:
    cloud.task_scripts["create_volume_group"] = ["QUEUED", "RUNNING", "SUCCEEDED"]

    await target_factory().create_volume_group("g", "cluster", "container", [])

    assert cloud.polls == 3
    assert clock.now == 2.0  # it slept between the three polls, not after the last


@pytest.mark.parametrize("status", ["FAILED", "CANCELED"])
async def test_wait_raises_for_a_failed_or_canceled_task(
    cloud: _Cloud, target_factory: Callable[..., Any], status: str
) -> None:
    cloud.task_scripts["create_volume_group"] = [status]

    with pytest.raises(RuntimeError, match=rf"task-\d+ {status}: legacy boom"):
        await target_factory().create_volume_group("g", "cluster", "container", [])


async def test_wait_gives_up_on_a_task_that_never_finishes_and_asks_it_to_cancel(
    cloud: _Cloud, clock: _Clock, target_factory: Callable[..., Any]
) -> None:
    cloud.task_scripts["create_volume_group"] = ["RUNNING"]

    with pytest.raises(TimeoutError, match=r"task-\d+ still RUNNING after 5 s"):
        await target_factory(task_timeout_seconds=5.0).create_volume_group("g", "cluster", "container", [])

    assert cloud.names().count("cancel_task") == 1
    assert clock.now == 5.0


async def test_the_rollback_of_a_timed_out_create_waits_for_the_cancel_and_removes_what_the_task_made(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.task_scripts["create_volume_group"] = ["RUNNING"]

    with pytest.raises(TimeoutError, match=r"Nutanix task .* still RUNNING after"):
        await target_factory(task_timeout_seconds=5.0).create_volume_group("g", "cluster", "container", [])

    assert cloud.groups == {}


async def test_the_rollback_of_a_create_that_will_not_end_reports_the_task_instead_of_deleting(
    cloud: _Cloud, clock: _Clock, target_factory: Callable[..., Any]
) -> None:
    cloud.task_scripts["create_volume_group"] = ["RUNNING"]
    cloud.cancel_ends_task = False

    with pytest.raises(TimeoutError, match=r"Nutanix task .* still RUNNING after") as caught:
        await target_factory(task_timeout_seconds=5.0).create_volume_group("g", "cluster", "container", [])

    assert len(cloud.groups) == 1  # not deleted: what the task makes is unknown while it runs
    assert caught.value.__notes__ == [f"rollback step failed: {caught.value.args[0]}"]
    assert clock.now == 10.0  # the wait, then the same again before the rollback gives up


async def test_a_failed_cancel_request_does_not_hide_the_timeout(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.task_scripts["create_volume_group"] = ["RUNNING"]
    cloud.fail_calls["cancel_task"] = RuntimeError("cannot cancel")

    with pytest.raises(TimeoutError, match="still RUNNING"):
        await target_factory(task_timeout_seconds=5.0).create_volume_group("g", "cluster", "container", [])


async def test_wait_accepts_a_task_that_succeeds_on_its_last_poll_before_the_deadline(
    cloud: _Cloud, clock: _Clock, target_factory: Callable[..., Any]
) -> None:
    cloud.task_scripts["create_volume_group"] = ["RUNNING"] * 4 + ["SUCCEEDED"]

    group = await target_factory(task_timeout_seconds=5.0).create_volume_group("g", "cluster", "container", [])

    assert list(cloud.groups) == [group.ext_id]
    assert "cancel_task" not in cloud.names()
    assert clock.now == 4.0  # one 1 s poll after each RUNNING


async def test_wait_prefers_the_structured_error_messages(cloud: _Cloud, target_factory: Callable[..., Any]) -> None:
    cloud.task_scripts["create_volume_group"] = ["FAILED"]
    cloud.error_messages = ["structured message"]

    with pytest.raises(RuntimeError, match="structured message"):
        await target_factory().create_volume_group("g", "cluster", "container", [])


# -- create_volume_group -------------------------------------------------------


async def test_create_volume_group_returns_the_ids_its_create_tasks_named_in_creation_order(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    group = await target_factory().create_volume_group("vg-name", "cluster", "container", [10, 20, 30])

    assert group.ext_id in cloud.groups and group.target_name == f"target-{group.ext_id}"
    created = tuple(disk.ext_id for disk in cloud.groups[group.ext_id]["disks"])
    assert group.disk_ext_ids == created and len(set(created)) == 3
    assert "delete_volume_group_by_id" not in cloud.names()


async def test_create_volume_group_reports_each_disk_with_its_index_and_size_as_the_sdk_says(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    group = await target_factory().create_volume_group("vg-name", "cluster", "container", [10, 20, 30])

    assert [(d.index, d.size_bytes) for d in group.disks] == [(0, 10), (1, 20), (2, 30)]
    assert [d.ext_id for d in group.disks] == list(group.disk_ext_ids)
    read_back = [entry[2] for entry in cloud.log if entry[0] == "get_volume_disk_by_id"]
    assert read_back == list(group.disk_ext_ids)  # each disk was read by its own id


async def test_create_volume_group_takes_the_index_from_what_the_disk_reports_not_what_was_asked(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.reported_index_offset = 10

    group = await target_factory().create_volume_group("vg-name", "cluster", "container", [10, 20])

    assert [d.index for d in group.disks] == [10, 11]


async def test_create_volume_group_takes_the_size_from_what_the_disk_reports_not_what_was_asked(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.reported_size_offset = 4096

    group = await target_factory().create_volume_group("vg-name", "cluster", "container", [10, 20])

    assert [d.size_bytes for d in group.disks] == [10 + 4096, 20 + 4096]


@pytest.mark.parametrize("missing", ["report_no_index", "report_no_size"])
async def test_create_volume_group_refuses_a_disk_that_reports_no_index_or_size(
    cloud: _Cloud, target_factory: Callable[..., Any], missing: str
) -> None:
    setattr(cloud, missing, True)

    with pytest.raises(RuntimeError, match=r"Volume Group disk disk-\d+ reports no index or size"):
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])

    assert cloud.groups == {}  # and the group made so far is removed


async def test_create_volume_group_needs_the_task_to_name_the_group(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.omit_entities = True

    with pytest.raises(RuntimeError, match=r"task-\d+ finished without exactly one reference to the Volume Group"):
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])


async def test_create_volume_group_needs_each_disk_task_to_name_its_disk(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    class NoDiskRef(_FakeVolumeGroups):
        def create_volume_disk(self, group_id: str, body: Any) -> SimpleNamespace:
            response = super().create_volume_disk(group_id, body)
            cloud.task_created[response.data.ext_id] = [(group_id, _VG_REL)]  # the group only, no disk
            return response

    target = target_factory()
    target._sdk.volume_groups = NoDiskRef(cloud)

    with pytest.raises(RuntimeError, match="the Volume Group disk it created"):
        await target.create_volume_group("vg-name", "cluster", "container", [10])

    assert cloud.groups == {}  # the group made before the failure is removed, by its id


@pytest.mark.parametrize("order", ["group_first", "disk_first"])
async def test_create_volume_group_takes_the_disk_that_is_not_the_group_whatever_the_order(
    cloud: _Cloud, target_factory: Callable[..., Any], order: str
) -> None:
    if order == "disk_first":
        _list_instead(cloud, "create_volume_disk", lambda entities: entities[::-1])

    group = await target_factory().create_volume_group("vg-name", "cluster", "container", [10, 20])

    assert group.disk_ext_ids == tuple(d.ext_id for d in cloud.groups[group.ext_id]["disks"])


async def test_create_volume_group_refuses_a_disk_task_that_names_two_disks(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    _list_instead(cloud, "create_volume_disk", lambda e: [*e, ("another-disk", "volumes:config:volume-group:disk")])

    with pytest.raises(
        RuntimeError, match=r"Volume Group disk it created \(found 2 ending with ':volume-group:disk'\)"
    ):
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])

    assert cloud.groups == {}


async def test_create_volume_group_ignores_entities_of_other_types_in_its_tasks(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    _list_instead(cloud, "create_volume_group", lambda e: [*e, ("a-client", "volumes:config:iscsi-client")])
    _list_instead(cloud, "create_volume_disk", lambda e: [*e, ("a-vm", "vmm:ahv:config:vm")])

    group = await target_factory().create_volume_group("vg-name", "cluster", "container", [10, 20])

    assert group.ext_id in cloud.groups
    assert group.disk_ext_ids == tuple(d.ext_id for d in cloud.groups[group.ext_id]["disks"])


async def test_create_volume_group_needs_a_reference_whose_rel_ends_with_volume_group(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    _list_instead(cloud, "create_volume_group", lambda e: [(e[0][0], "volumes:config:something-else")])

    with pytest.raises(RuntimeError, match=r"Volume Group it created \(found 0 ending with ':volume-group'\)"):
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])


async def test_create_volume_group_needs_a_disk_reference_whose_rel_ends_with_volume_group_disk(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    _list_instead(cloud, "create_volume_disk", lambda e: [(i, "volumes:config:volume-group:vdisk") for i, _ in e])

    with pytest.raises(
        RuntimeError, match=r"Volume Group disk it created \(found 0 ending with ':volume-group:disk'\)"
    ):
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])

    assert cloud.groups == {}  # the group made before is still removed


async def test_a_volume_group_disk_task_ignores_the_group_and_a_vm_disk_by_their_rel(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    _list_instead(cloud, "create_volume_disk", lambda e: [*e, ("a-vm-disk", "vmm:ahv:config:vm:disk")])

    group = await target_factory().create_volume_group("vg-name", "cluster", "container", [10, 20])

    # The task listed the group (':volume-group'), the new disk (':volume-group:disk') and a VM disk
    # (':vm:disk'); only the second is a Volume Group disk.
    assert group.disk_ext_ids == tuple(d.ext_id for d in cloud.groups[group.ext_id]["disks"])
    assert "a-vm-disk" not in group.disk_ext_ids and group.ext_id not in group.disk_ext_ids


async def test_a_group_labelled_as_a_volume_group_disk_makes_the_task_untrustworthy(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    _list_instead(cloud, "create_volume_disk", lambda e: [(i, "volumes:config:volume-group:disk") for i, _ in e])

    # Nothing excludes the group by id: two entities now claim to be the disk, so the task is refused.
    with pytest.raises(
        RuntimeError, match=r"Volume Group disk it created \(found 2 ending with ':volume-group:disk'\)"
    ):
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])

    assert cloud.groups == {}


async def test_the_rollback_of_a_volume_group_removes_only_the_entity_ending_with_volume_group(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    _list_instead(cloud, "create_volume_group", lambda e: [*e, ("a-client", "volumes:config:iscsi-client")])
    cloud.task_scripts["create_volume_disk"] = ["FAILED"]

    with pytest.raises(RuntimeError, match="FAILED") as caught:
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])

    assert cloud.groups == {}  # the group was removed although its task listed something else as well
    assert getattr(caught.value, "__notes__", []) == []


async def test_create_volume_group_removes_the_group_when_a_disk_task_fails(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.task_scripts["create_volume_disk"] = ["FAILED"]

    with pytest.raises(RuntimeError, match="FAILED"):
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])

    assert cloud.groups == {}


async def test_create_volume_group_removes_the_group_when_a_disk_call_raises(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.fail_calls["create_volume_disk"] = ValueError("no space")

    with pytest.raises(ValueError, match="no space"):
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])

    assert cloud.groups == {}


async def test_create_volume_group_removes_a_group_a_failed_task_still_named(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.task_scripts["create_volume_group"] = ["FAILED"]

    with pytest.raises(RuntimeError, match="FAILED"):
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])

    assert cloud.groups == {}  # a failed task that reports an entity it made does not leave it behind


async def test_create_volume_group_has_nothing_to_remove_when_the_create_call_fails(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.fail_calls["create_volume_group"] = ConnectionError("create failed")

    with pytest.raises(ConnectionError, match="create failed"):
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])

    assert "delete_volume_group_by_id" not in cloud.names()


async def test_a_failed_cleanup_is_attached_to_the_original_error(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.fail_calls["create_volume_disk"] = ValueError("no space")
    cloud.fail_calls["delete_volume_group_by_id"] = RuntimeError("cleanup failed")

    with pytest.raises(ValueError, match="no space") as caught:
        await target_factory().create_volume_group("vg-name", "cluster", "container", [10])

    assert caught.value.__notes__ == ["rollback step failed: cleanup failed"]


# -- allow_initiator / create_vm -----------------------------------------------


async def test_allow_initiator_attaches_the_iqn_to_the_group_by_id(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    target = target_factory()
    group = await target.create_volume_group("vg-name", "cluster", "container", [10])

    await target.allow_initiator(group.ext_id, "iqn.host:me")

    assert [c.iqn for c in cloud.groups[group.ext_id]["attachments"]] == ["iqn.host:me"]


async def test_create_vm_returns_the_id_its_task_named(cloud: _Cloud, target_factory: Callable[..., Any]) -> None:
    vm_id = await _create_vm(target_factory(), "web-01")

    assert cloud.vms[vm_id]["name"] == "web-01"
    assert cloud.names()[0] == "create_vm"
    assert cloud.names() == ["create_vm"]  # no lookup of any kind by name


async def test_create_vm_is_not_confused_by_another_vm_of_the_same_name(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.vms["vm-someone-elses"] = {"name": "web-01"}  # AHV allows duplicate names

    vm_id = await _create_vm(target_factory(), "web-01")

    assert vm_id != "vm-someone-elses" and cloud.vms[vm_id]["name"] == "web-01"
    assert cloud.vms["vm-someone-elses"] == {"name": "web-01"}


async def test_create_vm_raises_when_the_task_fails(cloud: _Cloud, target_factory: Callable[..., Any]) -> None:
    cloud.task_scripts["create_vm"] = ["FAILED"]

    with pytest.raises(RuntimeError, match="FAILED"):
        await _create_vm(target_factory())


async def test_create_vm_does_not_guess_when_the_task_names_no_vm(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.omit_entities = True

    with pytest.raises(RuntimeError, match=r"task-\d+ finished without exactly one reference to the VM it created"):
        await _create_vm(target_factory())


async def test_create_vm_needs_a_reference_whose_rel_ends_with_vm(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    _list_instead(cloud, "create_vm", lambda e: [(e[0][0], "anything:at:all")])  # the VM itself, mislabelled

    with pytest.raises(RuntimeError, match=r"\(found 0 ending with ':vm'\)"):
        await _create_vm(target_factory(), "web-01")


async def test_create_vm_refuses_a_task_that_names_two_vms(cloud: _Cloud, target_factory: Callable[..., Any]) -> None:
    _list_instead(cloud, "create_vm", lambda e: [*e, ("vm-other", _VM_REL)])
    cloud.vms["vm-other"] = {"name": "another"}  # a second entity that is a VM too

    with pytest.raises(RuntimeError, match=r"\(found 2 ending with ':vm'\)"):
        await _create_vm(target_factory())


@pytest.mark.parametrize("order", ["vm_first", "disks_first"])
async def test_create_vm_finds_the_vm_among_the_disks_its_task_also_lists_in_any_order(
    cloud: _Cloud, target_factory: Callable[..., Any], order: str
) -> None:
    if order == "disks_first":
        _list_instead(cloud, "create_vm", lambda entities: entities[::-1])

    vm_id = await target_factory().create_vm(
        "web-01", "c", "k", _GROUP, [10, 20, 30], cpus=1, memory_bytes=2, uefi=False
    )

    assert cloud.vms[vm_id]["name"] == "web-01"


async def test_the_rollback_of_a_failed_create_vm_removes_the_vm_its_task_named(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.task_scripts["create_vm"] = ["FAILED"]

    with pytest.raises(RuntimeError, match="FAILED"):
        await target_factory().create_vm("web-01", "c", "k", _GROUP, [10, 20], cpus=1, memory_bytes=2, uefi=False)

    assert cloud.vms == {}


async def test_create_vm_refuses_a_task_that_lists_disks_but_no_vm(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    _list_instead(cloud, "create_vm", lambda e: e[1:])  # drop the VM itself

    with pytest.raises(RuntimeError, match=r"reference to the VM it created \(found 0 ending with ':vm'\)"):
        await target_factory().create_vm("web-01", "c", "k", _GROUP, [10], cpus=1, memory_bytes=2, uefi=False)


async def test_create_vm_counts_a_repeated_id_once(cloud: _Cloud, target_factory: Callable[..., Any]) -> None:
    _list_instead(cloud, "create_vm", lambda e: e * 2)

    vm_id = await _create_vm(target_factory())

    assert vm_id in cloud.vms


async def test_a_failed_create_task_that_names_nothing_leaves_no_rollback_noise(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.task_scripts["create_vm"] = ["FAILED"]
    cloud.omit_entities = True  # the failed task made nothing, so it lists nothing

    with pytest.raises(RuntimeError, match="FAILED") as caught:
        await _create_vm(target_factory())

    assert getattr(caught.value, "__notes__", []) == []  # there was nothing to roll back, so nothing to report


async def test_rollback_does_not_guess_when_a_failed_task_names_several_things(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    _list_instead(cloud, "create_vm", lambda e: [*e, ("vm-existing", "vmm:ahv:config:vm")])
    cloud.task_scripts["create_vm"] = ["FAILED"]
    cloud.vms["vm-existing"] = {"name": "someone-elses"}

    with pytest.raises(RuntimeError, match="FAILED") as caught:
        await _create_vm(target_factory())

    assert "vm-existing" in cloud.vms  # not deleted
    assert any("exactly one reference" in note for note in caught.value.__notes__)


# -- wait_hydrated -------------------------------------------------------------


async def test_wait_hydrated_sits_out_the_settle_period_when_a_version_never_reports_hydration(
    cloud: _Cloud, clock: _Clock, target_factory: Callable[..., Any]
) -> None:
    cloud.hydration_script = [None]

    assert await target_factory().wait_hydrated("vm") is True

    # The first look starts the quiet period at t=0; it counts only once 10 s have passed.
    assert clock.now == 10.0
    assert cloud.names().count("list_disks_by_vm_id") == 11


async def test_wait_hydrated_waits_while_a_disk_is_in_progress_and_then_settles(
    cloud: _Cloud, clock: _Clock, target_factory: Callable[..., Any]
) -> None:
    cloud.hydration_script = ["IN_PROGRESS"] * 4 + [None]

    assert await target_factory().wait_hydrated("vm") is True

    assert clock.now == 14.0  # four polls in progress, the quiet period starts at t=4 and lasts 10 s


async def test_wait_hydrated_restarts_the_settle_period_when_progress_shows_up_late(
    cloud: _Cloud, clock: _Clock, target_factory: Callable[..., Any]
) -> None:
    cloud.hydration_script = [None, SimpleNamespace(name="IN_PROGRESS"), None]

    assert await target_factory().wait_hydrated("vm") is True

    assert clock.now == 12.0  # quiet at t=0, progress at t=1 resets it, quiet again from t=2


async def test_wait_hydrated_gives_up_when_a_disk_never_finishes_hydrating(
    cloud: _Cloud, clock: _Clock, target_factory: Callable[..., Any]
) -> None:
    cloud.hydration_script = ["IN_PROGRESS"]

    with pytest.raises(TimeoutError, match="still hydrating after 30 s"):
        await target_factory(hydration_timeout_seconds=30.0).wait_hydrated("vm")

    assert clock.now == 30.0


async def test_wait_hydrated_raises_when_hydration_failed(cloud: _Cloud, target_factory: Callable[..., Any]) -> None:
    cloud.hydration_script = ["FAILED"]

    with pytest.raises(RuntimeError, match="failed to hydrate"):
        await target_factory().wait_hydrated("vm")


async def test_wait_hydrated_reports_a_disk_with_hydration_disabled(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.hydration_script = ["DISABLED"]

    assert await target_factory().wait_hydrated("vm") is False


# -- deletes: only what this run created ----------------------------------------


async def test_delete_volume_group_sends_the_etag_and_deletes_it_by_id(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    target = target_factory()
    group = await target.create_volume_group("vg-name", "cluster", "container", [10])

    await target.delete_volume_group(group.ext_id)

    assert cloud.groups == {}
    header = ("header", "volume_group", "If-Match", "etag-vg-name")
    assert cloud.log.index(header) < cloud.names().index("delete_volume_group_by_id")


async def test_delete_volume_group_detaches_iscsi_clients_first(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    target = target_factory()
    group = await target.create_volume_group("vg-name", "cluster", "container", [10])
    await target.allow_initiator(group.ext_id, "iqn.host:me")

    await target.delete_volume_group(group.ext_id)

    assert cloud.groups == {}
    assert cloud.names().index("detach_iscsi_client") < cloud.names().index("delete_volume_group_by_id")


async def test_delete_vm_deletes_a_vm_this_run_created(cloud: _Cloud, target_factory: Callable[..., Any]) -> None:
    target = target_factory()
    vm_id = await _create_vm(target)

    await target.delete_vm(vm_id)

    assert cloud.vms == {}
    assert ("header", "vm", "If-Match", "etag-vm") in cloud.log


async def test_delete_refuses_a_volume_group_this_run_did_not_create_and_touches_nothing(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.groups["vg-existing"] = {
        "name": "someone-elses",
        "disks": [],
        "attachments": [SimpleNamespace(ext_id="client-1", iqn="iqn.other")],
    }

    with pytest.raises(RuntimeError, match="refusing to delete vg-existing: it was not created by this run"):
        await target_factory().delete_volume_group("vg-existing")

    assert cloud.log == []  # not even a read, and in particular its iSCSI client was not detached
    assert len(cloud.groups["vg-existing"]["attachments"]) == 1


async def test_delete_refuses_a_vm_this_run_did_not_create(cloud: _Cloud, target_factory: Callable[..., Any]) -> None:
    cloud.vms["vm-existing"] = {"name": "web-01"}

    with pytest.raises(RuntimeError, match="refusing to delete vm-existing"):
        await target_factory().delete_vm("vm-existing")

    assert cloud.log == [] and "vm-existing" in cloud.vms


async def test_one_target_cannot_delete_what_another_created(cloud: _Cloud, target_factory: Callable[..., Any]) -> None:
    vm_id = await _create_vm(target_factory())

    with pytest.raises(RuntimeError, match="not created by this run"):
        await target_factory().delete_vm(vm_id)  # a fresh target has created nothing

    assert vm_id in cloud.vms


async def test_a_same_named_vm_does_not_make_its_id_deletable(
    cloud: _Cloud, target_factory: Callable[..., Any]
) -> None:
    cloud.vms["vm-someone-elses"] = {"name": "web-01"}
    target = target_factory()
    await _create_vm(target, "web-01")

    with pytest.raises(RuntimeError, match="vm-someone-elses"):
        await target.delete_vm("vm-someone-elses")

    assert "vm-someone-elses" in cloud.vms
