"""Shared plumbing of the hypervisor restore scripts: the repository side (resolve a ref, list its disks, detect
their firmware), the options every script takes, and how a run reports and undoes its steps.

Every script reads the backup through this SDK and writes each disk into the hypervisor's own storage (a full
restore); the backup is never written.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import functools
import getpass
import signal
import ssl
import struct
import sys
import time
import uuid
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass
from typing import Any, TextIO

from synology_apm_repo.sdk import (
    ApmRepoError,
    Node,
    NodeFrame,
    NodeRef,
    RefKind,
    Repository,
    RestorableUnit,
    Session,
    UnitKind,
    UnitProvider,
)
from synology_apm_repo.sdk.export import ExportResult, ExportSink, ExportTuning, run_export
from synology_apm_repo.sdk.presentation import Progress, ProgressMeter, format_bytes
from synology_apm_repo.sdk.profiles import store_from_profile

GIB = 1 << 30
BLOCK_SIZES = (512, 4096)
_GPT_MAX_TABLE_BYTES = 1 << 20
_ESP_TYPE_GUID = "c12a7328-f81f-11d2-ba4b-00a0c93ec93b"


class RestoreError(Exception):
    """A step of the restore failed; the message says which and why."""


# --------------------------------------------------------------------------- #
# Command line
# --------------------------------------------------------------------------- #


def add_common_args(parser: argparse.ArgumentParser, *, disk_bus: bool = True) -> None:
    """The options every script shares; ``disk_bus`` adds ``--disk-bus`` for a hypervisor that offers a choice."""
    parser.add_argument(
        "ref",
        help="<path>#<source>/<workload>/<version> (display names) or a canonical <path>#@cat:N/wl:N/ver:UID ref, "
        "exactly as synology-apm-repo-cli takes it (with --profile, <path> is store-relative); every disk at or "
        "below it is restored. VM, PC and PS versions all work.",
    )
    parser.add_argument(
        "--profile",
        help="open a saved connection profile (see `synology-apm-repo-cli profile add`); the ref's <path> is then a "
        "store-relative sub-path instead of a filesystem path",
    )
    parser.add_argument("--key", help="repository key '<userKeyID>@<base64 userKey>' if encrypted")
    parser.add_argument("--dry-run", action="store_true", help="list the source disks and the firmware, then stop")
    parser.add_argument("--vm-name", help="name of the new VM")
    parser.add_argument("--cpus", type=int, default=1, help="vCPUs of the new VM (default: 1)")
    parser.add_argument("--memory-gib", type=int, default=2, help="RAM of the new VM in GiB (default: 2)")
    parser.add_argument(
        "--firmware",
        choices=("auto", "uefi", "bios"),
        default="auto",
        help="boot firmware of the new VM; auto picks UEFI for a GPT disk with an EFI System Partition (default)",
    )
    if disk_bus:
        parser.add_argument(
            "--disk-bus",
            choices=("sata", "scsi"),
            default="sata",
            help="bus of the new VM's disks (default: sata, which a guest backed up from another hypervisor is most "
            "likely to have a driver for)",
        )
    parser.add_argument("--insecure", action="store_true", help="do not verify the hypervisor's TLS certificate")


def check_common_args(parser: argparse.ArgumentParser, args: argparse.Namespace, required: tuple[str, ...]) -> None:
    """Rejects what the shared options cannot do. ``required`` names the script's own options that every run but a
    dry run needs."""
    if args.dry_run:
        return
    missing = [f"--{name.replace('_', '-')}" for name in ("vm_name", *required) if not getattr(args, name)]
    if missing:
        parser.error(f"required unless --dry-run: {', '.join(missing)}")
    if args.cpus < 1 or args.memory_gib < 1:
        parser.error("--cpus and --memory-gib must be at least 1")


def resolve_password(
    env: Mapping[str, str], variable: str, what: str, *, dry_run: bool, prompt: Callable[[str], str] = getpass.getpass
) -> str:
    """``variable`` from ``env``, else a prompt; a dry run needs none."""
    return env.get(variable) or ("" if dry_run else prompt(f"{what} password: "))


def parse_ref(value: str) -> NodeRef:
    """A bare path is accepted as a human ref with no segments (and then fails in ``resolve_start``)."""
    return NodeRef.parse(value) if "#" in value else NodeRef.human(value)


def restore_name(prefix: str, vm_name: str) -> str:
    """A name for the temporary objects of one run: only characters every hypervisor accepts."""
    safe = "".join(c if (c.isascii() and c.isalnum()) or c in "-_" else "-" for c in vm_name)[:40]
    return f"{prefix}-{safe}-{uuid.uuid4().hex[:6]}"


# --------------------------------------------------------------------------- #
# Repository side
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SourceDisk:
    label: str
    unit: RestorableUnit
    size: int


async def resolve_start(repo: Repository, ref: NodeRef) -> tuple[UnitProvider, Node]:
    """The provider and node a ``<path>#<source>/<workload>/<version>[/...]`` ref names."""
    if ref.kind is RefKind.RAW:
        raise SystemExit("raw refs are not supported")
    frame = await repo.locate(ref)
    if not isinstance(frame, NodeFrame):
        raise SystemExit("REF must reach a backup version: <path>#<source>/<workload>/<version>")
    return frame.provider, frame.node


async def collect_disks(provider: UnitProvider, start: Node) -> list[SourceDisk]:
    """Every disk image at or below ``start``: a VM's devices, a PC/PS version's disks, or one disk itself."""
    found: list[tuple[str, Node]] = []

    async def visit(node: Node, path: tuple[str, ...]) -> None:
        if node.is_leaf:
            if node.kind == UnitKind.DISK_IMAGE:
                found.append(("/".join(path) or node.name, node))
            return
        for child in await provider.children(node):
            # A disk's "(filesystem)" browse container holds files, not disks; walking it reads the whole tree.
            if child.kind != UnitKind.DISK_FILESYSTEM:
                await visit(child, (*path, child.name) if node is not start else (child.name,))

    await visit(start, ())
    disks = []
    for label, node in found:
        if node.size is None:
            raise SystemExit(f"disk {label!r} has no known size")
        disks.append(SourceDisk(label, await provider.unit(node), node.size))
    if not disks:
        raise SystemExit("REF contains no restorable disk images")
    return disks


async def detect_firmware(disk: SourceDisk) -> str:
    """``"uefi"`` when the disk carries a GPT with an EFI System Partition, else ``"bios"``: a disk boots only
    under the firmware it was installed for. A GPT header with an implausible table geometry counts as corrupt:
    ``"bios"``, with a warning, and none of its sizes is trusted (``--firmware`` overrides)."""
    content = disk.unit.content
    head = await content.read(0, 1 << 20)
    sector = next((size for size in BLOCK_SIZES if head[size : size + 8] == b"EFI PART"), None)
    if sector is None:
        return "bios"
    entries_lba, count, entry_size = struct.unpack("<QII", head[sector + 72 : sector + 88])
    table_bytes = count * entry_size
    if not (
        entries_lba >= 2
        and count > 0
        and 128 <= entry_size <= 4096
        and entry_size % 8 == 0
        and table_bytes <= _GPT_MAX_TABLE_BYTES
        and entries_lba * sector + table_bytes <= disk.size
    ):
        warn(
            f"the GPT header of {disk.label!r} looks corrupt (entries at LBA {entries_lba}, "
            f"{count} x {entry_size} B); assuming legacy BIOS"
        )
        return "bios"
    table = head[entries_lba * sector : entries_lba * sector + table_bytes]
    if len(table) < table_bytes:
        table = await content.read(entries_lba * sector, table_bytes)
    for index in range(count):
        if str(uuid.UUID(bytes_le=bytes(table[index * entry_size : index * entry_size + 16]))) == _ESP_TYPE_GUID:
            return "uefi"
    return "bios"


@dataclass(frozen=True)
class RestorePlan:
    disks: list[SourceDisk]
    firmware: str  # "uefi" or "bios"


async def open_repository(session: Session, ref: NodeRef, *, profile: str | None, key: str | None) -> Repository:
    """The one repository ``ref`` points into, opened (through a saved ``profile`` when given) and unlocked."""
    async with stage("opening repository"):
        if profile:
            repos = await session.open(await store_from_profile(profile), root=ref.repo_path)
        else:
            repos = await session.open(ref.repo_path)
    if len(repos) != 1:
        raise SystemExit(f"expected one repository at {ref.repo_path!r}, found {len(repos)}")
    repo = repos[0]
    if repo.is_encrypted:
        if not key:
            raise SystemExit("the repository is encrypted; pass --key")
        if not (await repo.set_key(key)).verification.ok:
            raise SystemExit("the repository key was rejected")
    return repo


async def plan_restore(repo: Repository, ref: NodeRef, *, firmware: str) -> RestorePlan:
    """The disks ``ref`` covers and the firmware to give the new VM (``"auto"`` detects it from the first disk)."""
    async with stage("resolving ref and listing disks"):
        provider, start = await resolve_start(repo, ref)
        disks = await collect_disks(provider, start)
    return RestorePlan(disks, firmware if firmware != "auto" else await detect_firmware(disks[0]))


def describe_plan(plan: RestorePlan, *, requested_firmware: str) -> None:
    for index, disk in enumerate(plan.disks, 1):
        print(f"disk {index}/{len(plan.disks)}: {disk.label}  {format_bytes(disk.size)}")
    detected = " (detected from the first disk)" if requested_firmware == "auto" else ""
    print(f"firmware: {plan.firmware.upper()}{detected}")


# --------------------------------------------------------------------------- #
# Exporting a disk
# --------------------------------------------------------------------------- #


class DiskProgress:
    """Progress line for one disk, rewritten in place on a tty. It counts planned bytes, so the holes and zero
    ranges a sparse export skips are not in it."""

    def __init__(self, prefix: str, stream: TextIO | None = None) -> None:
        self._prefix = prefix
        self._stream = stream if stream is not None else sys.stderr
        self._tty = self._stream.isatty()
        self._meter = ProgressMeter(self._render, min_interval=0.5 if self._tty else 10.0)

    async def update(self, done: int, total: int) -> None:
        await self._meter.update(Progress(phase="reading", determinate=True, done=done, total=total, unit="bytes"))

    def _emit(self, text: str, *, final: bool) -> None:
        if self._tty:
            print(f"\r{text}\033[K", end="\n" if final else "", file=self._stream, flush=True)
        else:
            print(text, file=self._stream, flush=True)

    async def _render(self, progress: Progress) -> None:
        self._emit(self.line(progress), final=False)

    def line(self, progress: Progress) -> str:
        done, total = progress.done, progress.total or 0
        shown = self._meter.formatted("bytes")
        parts = [
            self._prefix,
            f"{100 * done // total if total else 100:3d}%",
            f"{format_bytes(done)}/{format_bytes(total)}",
            shown.rate,
            f"ETA {shown.eta}" if shown.eta else "",
            f"elapsed {shown.elapsed}",
        ]
        return "  ".join(part for part in parts if part)

    def finish(self) -> None:
        latest = self._meter.latest
        if latest is not None:
            self._emit(self.line(latest), final=True)


async def export_disk(
    disk: SourceDisk, sink: ExportSink, label: str, *, tuning: ExportTuning | None = None
) -> ExportResult:
    """``run_export`` of ``disk`` into ``sink`` (sparse), with a progress line and a closing summary."""
    progress = DiskProgress(label)
    started = time.monotonic()
    result = await run_export(disk.unit.content, sink, sparse=True, progress=progress.update, tuning=tuning)
    progress.finish()
    seconds = max(time.monotonic() - started, 1e-9)
    print(
        f"{label}: wrote {format_bytes(result.bytes_written)} of {format_bytes(result.logical_size)} in "
        f"{seconds:.0f}s ({format_bytes(int(result.bytes_written / seconds))}/s)",
        file=sys.stderr,
    )
    return result


# --------------------------------------------------------------------------- #
# Running the steps
# --------------------------------------------------------------------------- #


class Rollback:
    """Undo steps for the resources created so far. If the ``async with`` body raises (Ctrl-C included), they
    run newest-first; a step that fails is attached to the original error as a note, so the original error is
    the one that surfaces."""

    def __init__(self) -> None:
        self._steps: list[Callable[[], Awaitable[object]]] = []

    def add(self, step: Callable[..., Awaitable[object]], *args: Any) -> None:
        self._steps.append(functools.partial(step, *args))

    def clear(self) -> None:
        """The body succeeded: nothing to undo."""
        self._steps.clear()

    async def __aenter__(self) -> Rollback:
        return self

    async def __aexit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        if exc is not None:
            for step in reversed(self._steps):
                try:
                    await step()
                except Exception as failure:  # noqa: BLE001
                    exc.add_note(f"rollback step failed: {failure}")


@contextlib.asynccontextmanager
async def stage(label: str, *, tick_seconds: float | None = None) -> AsyncIterator[None]:
    """Announces a slow step and reports its elapsed time while it runs: a remote repository can take minutes to
    open or list, and silence looks like a hang."""
    started = time.monotonic()
    interval = tick_seconds if tick_seconds is not None else (5.0 if sys.stderr.isatty() else 15.0)

    async def tick() -> None:
        while True:
            await asyncio.sleep(interval)
            print(f"{label}... {time.monotonic() - started:.0f}s elapsed", file=sys.stderr, flush=True)

    print(f"{label}...", file=sys.stderr, flush=True)
    ticker = asyncio.create_task(tick())
    try:
        yield
    finally:
        ticker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ticker
    print(f"{label}: {time.monotonic() - started:.0f}s", file=sys.stderr, flush=True)


async def wait_until[T](
    probe: Callable[[], Awaitable[T | None]],
    *,
    what: str,
    limit: float | None,
    interval: float = 2.0,
) -> T:
    """Awaits ``probe`` every ``interval`` seconds until it returns something other than ``None``, and returns that.

    Raises:
        RestoreError: Still waiting after ``limit`` seconds (``None``: no limit).
    """
    try:
        async with asyncio.timeout(limit):
            while True:
                result = await probe()
                if result is not None:
                    return result
                await asyncio.sleep(interval)
    except TimeoutError:
        raise RestoreError(f"{what}: still not done after {limit:.0f}s") from None


def tls_context(*, insecure: bool) -> ssl.SSLContext:
    """The TLS context of the hypervisor's HTTPS endpoints; ``insecure`` checks no certificate."""
    context = ssl.create_default_context()
    if insecure:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def warn(message: str) -> None:
    print(f"warning: {message}", file=sys.stderr, flush=True)


def run_main(
    restore: Callable[[argparse.Namespace, str], Coroutine[Any, Any, None]], args: argparse.Namespace, password: str
) -> None:
    """Runs ``restore`` and turns a failed step into a one-line error. Ctrl-C and SIGTERM cancel it, so what the
    run created is removed by its ``Rollback``; a rollback step that fails is printed either way."""
    if args.insecure:
        # urllib3 warns on every request; --insecure already says the certificate is not checked.
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    left_behind: list[str] = []

    async def cancellable() -> None:
        task = asyncio.current_task()
        assert task is not None
        with contextlib.suppress(NotImplementedError):  # no signal handlers on Windows' event loop
            asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
        try:
            await restore(args, password)
        except asyncio.CancelledError as exc:
            # asyncio.run raises a new exception for a cancelled task, without these notes.
            left_behind.extend(getattr(exc, "__notes__", []))
            raise

    started = time.monotonic()
    try:
        asyncio.run(cancellable())
    except (ApmRepoError, RestoreError) as exc:
        for note in getattr(exc, "__notes__", []):
            print(note, file=sys.stderr)
        raise SystemExit(f"error: {exc}") from exc
    except (KeyboardInterrupt, asyncio.CancelledError):
        for note in left_behind:
            print(note, file=sys.stderr)
        outcome = (
            "some of what this run created was left behind" if left_behind else "what this run created was removed"
        )
        raise SystemExit(f"interrupted; {outcome}") from None
    print(f"done in {time.monotonic() - started:.0f}s", file=sys.stderr)
