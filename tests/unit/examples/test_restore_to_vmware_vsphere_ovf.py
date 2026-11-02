"""Tests for the OVF descriptor examples/hypervisor-restore/restore_to_vmware_vsphere.py imports a VM from:
``ovf_descriptor``, ``unit_numbers``, ``disk_file`` and ``upload_url``."""

from __future__ import annotations

import argparse
import xml.etree.ElementTree as ET
from types import ModuleType
from typing import Any

import pytest

from unit.examples.hypervisor_restore_fakes import GIB, make_plan

_NS = {
    "ovf": "http://schemas.dmtf.org/ovf/envelope/1",
    "rasd": "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_ResourceAllocationSettingData",
    "vssd": "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_VirtualSystemSettingData",
    "vmw": "http://www.vmware.com/schema/ovf",
}
_OVF = "{http://schemas.dmtf.org/ovf/envelope/1}"
_VMW = "{http://www.vmware.com/schema/ovf}"


def _args(**overrides: Any) -> argparse.Namespace:
    values = {"vm_name": "apm-test-vm", "cpus": 2, "memory_gib": 4, "disk_bus": "sata", "network": None}
    return argparse.Namespace(guest_id="otherGuest64", **(values | overrides))


def _items(envelope: ET.Element) -> list[dict[str, str]]:
    """Each hardware item's ``rasd`` fields, by their local names."""
    return [
        {field.tag.split("}")[1]: field.text or "" for field in item}
        for item in envelope.iterfind(".//ovf:VirtualHardwareSection/ovf:Item", _NS)
    ]


def _parse(ex: ModuleType, plan: Any, **overrides: Any) -> ET.Element:
    return ET.fromstring(ex.ovf_descriptor(_args(**overrides), plan))


def test_descriptor_has_one_stream_optimized_disk_per_source_disk(ex: ModuleType) -> None:
    plan = make_plan(ex, 10 * GIB, 3 * GIB + 512)
    envelope = _parse(ex, plan)

    files = [(f.get(f"{_OVF}id"), f.get(f"{_OVF}href")) for f in envelope.iterfind(".//ovf:File", _NS)]
    disks = envelope.findall(".//ovf:Disk", _NS)
    assert files == [("file0", ex.disk_file(0)), ("file1", ex.disk_file(1))]
    assert [(d.get(f"{_OVF}fileRef"), d.get(f"{_OVF}capacity")) for d in disks] == [
        ("file0", str(10 * GIB)),
        ("file1", str(3 * GIB + 512)),
    ]
    assert {d.get(f"{_OVF}capacityAllocationUnits") for d in disks} == {"byte"}
    assert {d.get(f"{_OVF}format") for d in disks} == {
        "http://www.vmware.com/interfaces/specifications/vmdk.html#streamOptimized"
    }


def test_descriptor_sets_the_hardware(ex: ModuleType) -> None:
    envelope = _parse(ex, make_plan(ex, GIB), cpus=3, memory_gib=6)

    items = {item["ResourceType"]: item for item in _items(envelope)}
    assert items["3"]["VirtualQuantity"] == "3"
    assert (items["4"]["VirtualQuantity"], items["4"]["AllocationUnits"]) == ("6144", "byte * 2^20")
    system = envelope.find(".//ovf:System/vssd:VirtualSystemType", _NS)
    assert system is not None and system.text == "vmx-13"
    assert envelope.find(".//ovf:Name", _NS) is not None
    os_section = envelope.find(".//ovf:OperatingSystemSection", _NS)
    assert os_section is not None and os_section.get(f"{_VMW}osType") == "otherGuest64"


@pytest.mark.parametrize(
    ("bus", "sub_type", "resource_type"), [("sata", "vmware.sata.ahci", "20"), ("scsi", "VirtualSCSI", "6")]
)
def test_descriptor_puts_the_disks_on_one_controller(
    ex: ModuleType, bus: str, sub_type: str, resource_type: str
) -> None:
    envelope = _parse(ex, make_plan(ex, *[GIB] * 9), disk_bus=bus)

    items = _items(envelope)
    (controller,) = [i for i in items if i["ResourceType"] == resource_type]
    disks = [i for i in items if i["ResourceType"] == "17"]
    assert controller["ResourceSubType"] == sub_type
    assert {d["Parent"] for d in disks} == {controller["InstanceID"]}
    assert [d["HostResource"] for d in disks] == [f"ovf:/disk/vmdisk{i}" for i in range(9)]
    assert [int(d["AddressOnParent"]) for d in disks] == ex.unit_numbers(9, bus)


@pytest.mark.parametrize(("firmware", "value"), [("uefi", "efi"), ("bios", "bios")])
def test_descriptor_sets_the_firmware(ex: ModuleType, firmware: str, value: str) -> None:
    envelope = _parse(ex, make_plan(ex, GIB, firmware=firmware))

    (config,) = envelope.findall(".//vmw:Config", _NS)
    assert (config.get(f"{_VMW}key"), config.get(f"{_VMW}value")) == ("firmware", value)


def test_descriptor_adds_a_vmxnet3_nic_only_with_a_network(ex: ModuleType) -> None:
    without = _parse(ex, make_plan(ex, GIB))
    with_network = _parse(ex, make_plan(ex, GIB), network="VM Network")

    assert without.find("ovf:NetworkSection", _NS) is None
    assert not [i for i in _items(without) if i["ResourceType"] == "10"]
    network = with_network.find("ovf:NetworkSection/ovf:Network", _NS)
    assert network is not None and network.get(f"{_OVF}name") == "net0"
    (nic,) = [i for i in _items(with_network) if i["ResourceType"] == "10"]
    assert (nic["ResourceSubType"], nic["Connection"]) == ("VmxNet3", "net0")


def test_descriptor_escapes_the_vm_name(ex: ModuleType) -> None:
    name = 'web <01> & "db"'
    envelope = _parse(ex, make_plan(ex, GIB), vm_name=name)

    system = envelope.find("ovf:VirtualSystem", _NS)
    assert system is not None and system.get(f"{_OVF}id") == name
    assert envelope.findtext(".//ovf:VirtualSystem/ovf:Name", namespaces=_NS) == name


def test_unit_numbers_skip_the_scsi_controllers_own_unit(ex: ModuleType) -> None:
    assert ex.unit_numbers(8, "sata") == list(range(8))
    assert ex.unit_numbers(8, "scsi") == [0, 1, 2, 3, 4, 5, 6, 8]
    assert ex.unit_numbers(15, "scsi")[-1] == 15


@pytest.mark.parametrize(("count", "bus"), [(31, "sata"), (16, "scsi")])
def test_unit_numbers_refuse_more_disks_than_a_controller_holds(ex: ModuleType, count: int, bus: str) -> None:
    with pytest.raises(ex.RestoreError, match=f"{count} disks do not fit on one {bus.upper()} controller"):
        ex.unit_numbers(count, bus)


def test_upload_url_names_the_server_for_an_esxi_wildcard(ex: ModuleType) -> None:
    assert ex.upload_url("https://*/nfc/52a/disk-0.vmdk", "esxi.example.com") == (
        "https://esxi.example.com/nfc/52a/disk-0.vmdk"
    )
    assert ex.upload_url("https://*:8443/nfc/x", "10.0.0.5") == "https://10.0.0.5:8443/nfc/x"
    assert ex.upload_url("https://esxi-1.example.com/nfc/x", "vcenter") == "https://esxi-1.example.com/nfc/x"
