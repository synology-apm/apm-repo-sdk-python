"""Export the disks of a VM, PC or PS version from a backup repository into one zip file, compressed on the fly.

A zip entry can only be written front to back, but the exporter produces a disk's bytes in the order they sit in
the repository, not the order they sit on the disk. ``BufferedExportSink`` bridges the two: it takes the disk
in segments, in shared memory the export's worker processes write into, and hands each finished segment to
``ZipSink.flush_segment`` in disk order while the exporter is already filling the next one. So decoding (spread
over several cores) overlaps compressing (one thread), and the memory held is bounded however large the disk.

Flow:

1. Resolve a ref (``<path>#<source>/<workload>/<version>``, as the CLI takes it) and list every disk image at or
   below it; VM, PC and PS versions all qualify.
2. Write ``<output>.part``: one zip entry per disk, each exported with ``run_export`` into a ``ZipSink`` that owns
   the entry (the zip itself belongs to this script, which closes it).
3. Rename ``<output>.part`` to ``<output>``; on any failure it is deleted instead (a zip cannot drop a
   half-written entry).

Caveats:
    * Throughput is bounded by one deflate thread, which segment size and buffering cannot raise; ``--level 1``
      trades size for speed. Holes in a sparse disk are shipped as zeros (they compress to almost nothing), so
      the zip holds the whole logical disk.
    * The "waited for the zip" time each disk prints is how long the exporter was held back because the zip could
      not take the next segment: when it is most of the run, compression is the bottleneck. Larger segments
      (default 256 MiB) keep more worker processes busy; about ``--buffered-segments x --segment-size-mib`` of
      memory is held.

Requirements (the standard library plus this SDK)::

    pip install synology-apm-repo-sdk

Example::

    python examples/export_disk_to_zip.py \\
        "/backups/repo#MySource/web-01/2026-08-07 09:00:08" web-01.zip
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import re
import sys
import time
import zipfile
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO, TextIO

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
from synology_apm_repo.sdk.export import BufferedExportSink, FlushableSegment, run_export
from synology_apm_repo.sdk.presentation import Progress, ProgressMeter, format_bytes
from synology_apm_repo.sdk.profiles import store_from_profile

_MIB = 1 << 20


# --------------------------------------------------------------------------- #
# The destination: one zip entry, written in order
# --------------------------------------------------------------------------- #


class ZipSink(BufferedExportSink):
    """``ExportSink`` writing one disk into one entry of an open ``ZipFile``.

    The archive belongs to the caller, which opens it, closes it and deletes it when the export fails: a zip cannot
    drop a half-written entry, so this sink only finishes or abandons its own entry. Compression runs on a thread,
    so the event loop (and with it the export of the next segment) is not held up by it.
    """

    def __init__(self, archive: zipfile.ZipFile, entry_name: str, *, segment_size: int, buffered_segments: int) -> None:
        super().__init__(segment_size, max_buffered_segments=buffered_segments)
        self._archive = archive
        self._entry_name = entry_name
        self._entry: IO[bytes] | None = None

    async def create_destination(self, logical_size: int, *, sparse: bool) -> None:
        # By name, not a ``ZipInfo``: only a name takes the archive's compression and level. The size is not
        # known to ``ZipFile`` this way, so zip64 is forced (a disk over 4 GiB needs it).
        self._entry = await asyncio.to_thread(self._archive.open, self._entry_name, "w", force_zip64=True)

    async def flush_segment(self, segment: FlushableSegment) -> None:
        assert self._entry is not None
        async for block in segment.blocks():
            await asyncio.to_thread(self._entry.write, block)

    async def finalize_destination(self) -> None:
        await self._close_entry()

    async def discard_destination(self) -> bool:
        await self._close_entry()
        return False  # the caller deletes the whole zip

    async def _close_entry(self) -> None:
        entry, self._entry = self._entry, None
        if entry is not None:
            await asyncio.to_thread(entry.close)


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


def entry_names(disks: list[SourceDisk], output: Path) -> list[str]:
    """One distinct, path-safe entry name per disk: the output's own name for a single disk, else the disk's
    label with its position in front."""
    if len(disks) == 1:
        return [f"{output.stem}.img"]
    return [
        f"{position:02d}-{re.sub(r'[^A-Za-z0-9._-]+', '_', disk.label).strip('_')}.img"
        for position, disk in enumerate(disks, 1)
    ]


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


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #


class DiskProgress:
    """Progress line for one disk: percent, bytes, speed, ETA and elapsed (rate/ETA math is the SDK's
    ``ProgressMeter``). Counts planned bytes, so holes and zero ranges a sparse export skips are not in it.

    On a tty the line is rewritten in place every 0.5 s; otherwise a new line is printed every 10 s."""

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


@contextlib.asynccontextmanager
async def stage(label: str, *, tick_seconds: float | None = None) -> AsyncIterator[None]:
    """Announces a slow step and reports its elapsed time while it runs (a remote repository can take minutes to
    open or list, and silence looks like a hang). ``tick_seconds`` defaults to 5 s on a tty, else 15 s."""
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


def describe_disks(disks: list[SourceDisk], names: list[str]) -> None:
    for index, (disk, name) in enumerate(zip(disks, names, strict=True), 1):
        print(f"disk {index}/{len(disks)}: {disk.label}  {format_bytes(disk.size)}  -> {name}")


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ZipSettings:
    """What ``write_zip`` needs from the command line."""

    output: Path
    segment_size: int
    buffered_segments: int
    level: int

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> ZipSettings:
        return cls(
            output=Path(args.output),
            segment_size=args.segment_size_mib * _MIB,
            buffered_segments=args.buffered_segments,
            level=args.level,
        )


async def export_disk(
    archive: zipfile.ZipFile, disk: SourceDisk, name: str, index: int, total: int, settings: ZipSettings
) -> None:
    """``run_export`` of one disk into its own entry of ``archive``, with progress and a summary line."""
    label = f"disk {index + 1}/{total}"
    sink = ZipSink(archive, name, segment_size=settings.segment_size, buffered_segments=settings.buffered_segments)
    progress = DiskProgress(label)
    started = time.monotonic()
    result = await run_export(disk.unit.content, sink, sparse=True, progress=progress.update)
    progress.finish()
    seconds = max(time.monotonic() - started, 1e-9)
    stored = archive.getinfo(name).compress_size
    print(
        f"{label}: {format_bytes(result.logical_size)} into {format_bytes(stored)} "
        f"({100 * stored / max(result.logical_size, 1):.1f}%) in {seconds:.0f}s; "
        f"waited for the zip {result.sink_wait_seconds:.0f}s",
        file=sys.stderr,
    )


async def write_zip(disks: list[SourceDisk], names: list[str], settings: ZipSettings) -> None:
    """Writes every disk into ``settings.output``, which only appears once the whole zip is complete."""
    part = settings.output.with_name(settings.output.name + ".part")
    try:
        with zipfile.ZipFile(
            part, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=settings.level, allowZip64=True
        ) as archive:
            for index, (disk, name) in enumerate(zip(disks, names, strict=True)):
                await export_disk(archive, disk, name, index, len(disks), settings)
        part.replace(settings.output)
    except BaseException:
        part.unlink(missing_ok=True)
        raise


async def run(args: argparse.Namespace) -> None:
    ref = parse_ref(args.ref)
    settings = ZipSettings.from_args(args)
    async with Session() as session:
        repo = await open_repository(session, ref, profile=args.profile, key=args.key)
        async with stage("resolving ref and listing disks"):
            provider, start = await resolve_start(repo, ref)
            disks = await collect_disks(provider, start)
        names = entry_names(disks, settings.output)
        describe_disks(disks, names)
        if args.dry_run:
            return
        await write_zip(disks, names, settings)
        print(f"wrote {settings.output} ({format_bytes(settings.output.stat().st_size)})")


def parse_ref(value: str) -> NodeRef:
    """A bare path is accepted as a human ref with no segments (and then fails in ``resolve_start``)."""
    return NodeRef.parse(value) if "#" in value else NodeRef.human(value)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n", 1)[0] if __doc__ else None,
        epilog="Find refs with: synology-apm-repo-cli ls <path>[#<source>[/<workload>]] --ref",
    )
    parser.add_argument(
        "ref",
        help="<path>#<source>/<workload>/<version> (display names) or a canonical <path>#cat:N/wl:N/ver:UID ref, "
        "exactly as the CLI takes it (with --profile, <path> is store-relative); every disk at or below it is "
        "exported. VM, PC and PS versions all work.",
    )
    parser.add_argument("output", help="the zip file to write (replaced if it exists, with --force)")
    parser.add_argument(
        "--profile",
        help="open a saved connection profile (see `synology-apm-repo-cli profile add`); the ref's <path> is then a "
        "store-relative sub-path instead of a filesystem path",
    )
    parser.add_argument("--key", help="repository key '<userKeyID>@<base64 userKey>' if encrypted")
    parser.add_argument("--dry-run", action="store_true", help="list the source disks and stop")
    parser.add_argument("--force", action="store_true", help="replace OUTPUT if it already exists")
    parser.add_argument(
        "--segment-size-mib",
        type=int,
        default=256,
        help="MiB of a disk taken at once; larger keeps more worker processes busy, smaller uses less memory "
        "(default: 256)",
    )
    parser.add_argument(
        "--buffered-segments",
        type=int,
        default=2,
        help="segments held at once: one filling plus those waiting for or in compression (default: 2)",
    )
    parser.add_argument(
        "--level", type=int, default=6, choices=range(1, 10), help="deflate level, 1 fastest to 9 smallest (default: 6)"
    )
    args = parser.parse_args(argv)
    if args.segment_size_mib < 1:
        parser.error("--segment-size-mib must be at least 1")
    if args.buffered_segments < 1:
        parser.error("--buffered-segments must be at least 1")
    output = Path(args.output)
    if output.is_dir():
        parser.error(f"{args.output} is a directory")
    if output.exists() and not args.force and not args.dry_run:
        parser.error(f"{args.output} exists; pass --force to replace it")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    started = time.monotonic()
    try:
        asyncio.run(run(args))
    except ApmRepoError as exc:
        raise SystemExit(f"error: {exc}") from exc
    print(f"done in {time.monotonic() - started:.0f}s", file=sys.stderr)


if __name__ == "__main__":
    main()
