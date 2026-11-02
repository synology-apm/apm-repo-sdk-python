# Restoring a backup version to a hypervisor

Python scripts that restore the disks of one VM, PC or PS backup version
into a new VM. Each reads the backup with the SDK and writes the disks into
the hypervisor's storage itself (a full restore); a disk's unused blocks
are never sent.

| Script | Hypervisor | How a disk gets there |
|---|---|---|
| `restore_to_vmware_vsphere.py` | vSphere: ESXi, or vCenter | an OVF import, each disk uploaded as a streamOptimized VMDK into the import's NFC URL |
| `restore_to_proxmox_ve.py` | Proxmox VE | uploaded as a streamOptimized VMDK into a temporary `import` storage, then imported into the VM |
| `restore_to_nutanix_ahv.py` | Nutanix AHV (Prism Central) | written over iSCSI into a Volume Group disk, which the VM's disk is cloned from |
| `restore_to_qemu.py` | QEMU on this host | exported into a sparse raw file, or converted to qcow2 |

Each script's docstring lists its steps, caveats, requirements and an
example; `--help` lists its options, and `--dry-run` lists the disks and
the firmware without touching a hypervisor. The password comes from
`VSPHERE_PASSWORD`, `PVE_PASSWORD` or `NTNX_PASSWORD`, else a prompt. A
failed or interrupted run (Ctrl-C, SIGTERM) removes what it created.

`apm_restore_common.py` holds what the scripts share: the repository side,
the common options and the rollback. `stream_vmdk.py` writes a disk as a
streamOptimized VMDK while the export runs and sends it as one HTTP
request body, so no copy of a disk is staged on this host.

## Uploading a VMDK: stream or two-pass

`--upload stream` sends each VMDK once, chunked. `--upload two-pass` first
exports the disk only to count the VMDK's bytes, then uploads it with that
`Content-Length`: it costs a second read and compression of the disk's
data, not disk space.

| Hypervisor | Default | Measured |
|---|---|---|
| vSphere | `stream` | ESXi 6.7 accepted a chunked NFC upload; the vCenter 7 run used `two-pass` |
| Proxmox VE | `two-pass` | Proxmox VE 9.1's API server answers a chunked upload with HTTP 501 |
