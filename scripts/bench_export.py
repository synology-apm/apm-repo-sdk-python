"""Export benchmark against a real repository: ``bench_export.py run`` does one
measured export in this process, ``bench_export.py matrix`` repeats it in
fresh subprocesses and prints the median of each figure.

One run per process keeps ``ru_maxrss`` honest (it is a high-water mark for
the whole process), and a fresh process per run means nothing is cached in
the interpreter between runs; the OS page cache is warm after the first run,
so ``matrix`` discards ``--warmup`` leading runs.

Modes (``--mode``):

* ``default`` — ``export_range()`` into an unstaged ``LocalFileSink``:
  multiprocess when the store can be rebuilt in a worker, as the CLI and TUI
  exports are.
* ``inprocess`` — the same call with multiprocess dispatch disabled.
* ``stream`` — ``ContentSource.stream()`` copied into a file (no bucket-major
  planning; the baseline a user-written sink gets for free).
* ``sink-parent`` — ``export_range()`` into a sink that only accepts writes
  in the parent process, the shape of a single-handle disk API.
* ``segmented`` — ``run_export`` into a ``BufferedExportSink`` that discards
  each flushed segment (``--segment-mib``, ``--buffered-segments``, ``--storage``): what
  cutting an export into segments costs the exporter, with the destination free.
* ``plan-only`` / ``count-only`` — just the planning walk / the progress
  pre-pass, no decode, to see what they cost.

Usage::

    uv run python scripts/bench_export.py matrix --repo PATH \\
        --ref 'cat:3/wl:3/ver:<uid>/device:5/object:1' --repeat 5

``--sample NAME`` opens a ``[[remote_storage]]`` entry of the gitignored
``tests/smoke/smoke_samples.toml`` instead of ``--repo`` (then ``--ref`` is the
full ``<repo path>#<node ref>`` a browse prints). ``--count-calls`` counts this
process's ObjectStore calls and their time, so use it with ``--mode inprocess``:
tracing wraps the store, which turns multiprocess export off.

``--view-mib``/``--view-offset-mib`` export a window of the content instead
of all of it (a small export). Output goes to ``--workdir`` (a sparse file,
removed after each run); nothing is written into the repository.

POSIX only (``resource``, ``os.pwrite``).
"""

# mypy: disable-error-code="attr-defined"
from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import hashlib
import importlib
import json
import os
import resource
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

MODES = ("default", "inprocess", "stream", "sink-parent", "segmented", "plan-only", "count-only")

_MIB = 1 << 20
_DEV_NULL = Path(os.devnull)
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)


@dataclasses.dataclass
class RunResult:
    mode: str
    wall_s: float
    parent_cpu_s: float
    child_cpu_s: float
    parent_peak_rss_mib: float
    child_peak_rss_mib: float
    bytes_written: int
    logical_size: int
    holes: int
    zeros: int
    sha256: str | None = None
    store_calls: dict[str, list[float]] | None = None
    allocated_mib: float | None = None
    wchar_mib: float | None = None
    sink_wait_s: float | None = None

    @property
    def throughput_mib_s(self) -> float:
        return self.bytes_written / _MIB / self.wall_s if self.wall_s > 0 else 0.0


def _rss_mib(ru_maxrss: int) -> float:
    # macOS reports bytes, Linux kilobytes.
    return ru_maxrss / _MIB if sys.platform == "darwin" else ru_maxrss / 1024


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(8 * _MIB):
            digest.update(block)
    return digest.hexdigest()


async def _open_content(args: argparse.Namespace, calls: list[Any] | None) -> tuple[Any, Any]:
    from synology_apm_repo.sdk.api import Session
    from synology_apm_repo.sdk.units.node_ref import NodeRef

    # Tracing wraps the store, which turns multiprocess export off: trace only when counting calls.
    trace = calls.append if calls is not None else None

    session = Session()
    try:
        if args.sample:
            sys.path.insert(0, _REPO_ROOT)
            from synology_apm_repo.sdk.profiles import store_from_config

            # Imported by name: the smoke package is only importable with the repository root on sys.path.
            samples = importlib.import_module("tests.smoke._samples")
            sample = next((e for e in samples.load_smoke_samples() if getattr(e, "name", "") == args.sample), None)
            if not isinstance(sample, samples.RemoteStorageSample):
                raise SystemExit(f"no [[remote_storage]] sample named {args.sample!r}")
            store = await store_from_config(sample.config, sample.secrets)
            [repo] = await session.open(store, sample.key or None, root=sample.path, trace=trace)
            node_ref = NodeRef.parse(args.ref)
        else:
            [repo] = await session.open(args.repo, None, trace=trace)
            node_ref = NodeRef.parse(f"{args.repo}#{args.ref}")
        frame = await repo.resolve(node_ref)
        if not frame.node.is_leaf:
            raise SystemExit(f"{args.ref!r} names a folder, not a single item")
        return session, (await frame.unit()).content
    except BaseException:
        # An unclosed session's aiosqlite threads are non-daemon and would keep the process alive.
        await session.close()
        raise


def _wchar() -> int | None:
    """Bytes this process, and the workers it has already waited for, passed to write syscalls
    (Linux ``/proc/self/io``), else ``None``."""
    try:
        with open("/proc/self/io") as f:
            for line in f:
                if line.startswith("wchar:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return None


def _count_calls(calls: list[Any]) -> dict[str, list[float]]:
    """``{method: [calls, total seconds]}`` over this process's traced ObjectStore calls."""
    totals: dict[str, list[float]] = {}
    for event in calls:
        entry = totals.setdefault(event.method, [0, 0.0])
        entry[0] += 1
        entry[1] += event.elapsed
    return totals


def _fresh_output_dir(workdir: Path) -> Path:
    workdir.mkdir(parents=True, exist_ok=True)
    (workdir / "bench.out").unlink(missing_ok=True)
    return workdir


def _disable_multiprocess() -> None:
    from synology_apm_repo.sdk.dedup import export_scheduler

    class _NoDescriptor:
        @staticmethod
        def from_pool(pool: object) -> None:
            return None

    vars(export_scheduler)["PoolDescriptor"] = _NoDescriptor


def _set_workers(workers: int) -> None:
    from synology_apm_repo.sdk import concurrency
    from synology_apm_repo.sdk.dedup import export_scheduler

    vars(concurrency)["default_worker_count"] = lambda: workers
    vars(export_scheduler)["default_worker_count"] = lambda: workers


def _parent_only_sink(dst: Path, sink_mib_s: float = 0.0, sink_model: str = "queued") -> Any:
    """A sink whose writes must all happen in this process — a local file with
    its ``worker_target`` removed. ``sink_mib_s`` > 0 also caps how fast it
    accepts data, standing in for a slow destination (a network disk API):

    * ``queued`` — the cap applies in the sink's writer thread, behind its
      bounded queue (how ``LocalFileSink`` itself is built), so decoding and
      writing overlap until the queue fills.
    * ``inline`` — ``write_at`` itself waits, so the caller is blocked for
      the whole write and nothing overlaps (a sink that does its I/O
      synchronously in ``write_at``).
    """
    from synology_apm_repo.sdk.dedup import local_file_sink
    from synology_apm_repo.sdk.dedup.local_file_sink import LocalFileSink

    lock = threading.Lock()
    due = [0.0]

    burst_s = 0.1  # how far behind schedule the sink may fall and still catch up

    def _wait_turn(nbytes: int) -> float:
        # Token bucket: the n-th byte is not accepted before n / rate seconds after the first. Lateness (an
        # oversleep, an idle producer) is caught up within ``burst_s``, so the long-run rate is the cap.
        with lock:
            now = time.perf_counter()
            due[0] = max(due[0], now - burst_s) + nbytes / (sink_mib_s * _MIB)
            return due[0] - now

    if sink_mib_s > 0 and sink_model == "queued":
        real_pwrite = vars(local_file_sink)["pwrite"]

        def throttled_pwrite(fd: int, data: bytes | memoryview, offset: int) -> None:
            delay = _wait_turn(len(data))
            if delay > 0:
                time.sleep(delay)
            real_pwrite(fd, data, offset)

        vars(local_file_sink)["pwrite"] = throttled_pwrite

    class ParentOnlySink(LocalFileSink):
        def __init__(self, dst: Path) -> None:
            super().__init__(dst, staged=False)

        def worker_target(self) -> None:  # type: ignore[override]
            return None

        async def write_at(self, offset: int, data: bytes | memoryview) -> None:
            if sink_mib_s > 0 and sink_model == "inline":
                delay = _wait_turn(len(data))
                if delay > 0:
                    await asyncio.sleep(delay)
            await super().write_at(offset, data)

    return ParentOnlySink(dst)


def _discarding_segmented_sink(segment_size: int, buffered_segments: int, storage: Any) -> Any:
    """A ``BufferedExportSink`` whose flush throws the segment away."""
    from synology_apm_repo.sdk.dedup.buffered_export_sink import BufferedExportSink, FlushableSegment

    class DiscardingSink(BufferedExportSink):
        async def create_destination(self, logical_size: int, *, sparse: bool) -> None: ...

        async def flush_segment(self, segment: FlushableSegment) -> None: ...

        async def finalize_destination(self) -> None: ...

        async def discard_destination(self) -> bool:
            return False

    return DiscardingSink(segment_size, max_buffered_segments=buffered_segments, storage=storage)


async def _stream_to_file(content: Any, dst: Path) -> tuple[int, int]:
    size = content.size
    written = 0
    zeros = b""
    fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    try:
        os.ftruncate(fd, size)
        async for offset, block in content.stream():
            # Keep the output sparse, as export does. A comparison against a
            # zero buffer is one memcmp; any(block) would visit every byte in
            # Python and cost more than the read being measured.
            if len(zeros) < len(block):
                zeros = bytes(len(block))
            if block != zeros[: len(block)]:
                os.pwrite(fd, block, offset)
                written += len(block)
    finally:
        os.close(fd)
    return written, size


async def _measure(args: argparse.Namespace) -> RunResult:
    from synology_apm_repo.sdk.dedup.export_scheduler import ExportTuning
    from synology_apm_repo.sdk.dedup.export_sink import run_sink_export
    from synology_apm_repo.sdk.dedup.local_file_sink import LocalFileSink
    from synology_apm_repo.sdk.export import run_export

    if args.workers:
        _set_workers(args.workers)
    if args.mode == "inprocess":
        _disable_multiprocess()
    calls: list[Any] | None = [] if args.count_calls else None
    session, content = await _open_content(args, calls)
    try:
        if args.view_mib:
            content = content.view(args.view_offset_mib * _MIB, args.view_mib * _MIB)
        workdir = await asyncio.to_thread(_fresh_output_dir, Path(args.workdir))
        dst = _DEV_NULL if args.dev_null else workdir / "bench.out"

        kwargs: dict[str, Any] = {"sparse": not args.dense}
        if args.window_entries:
            kwargs["tuning"] = ExportTuning(window_entries=args.window_entries)

        async def on_bytes(written: int) -> None:
            return None

        async def progress(done: int, total: int) -> None:
            return None

        # export_range() reports per-write byte counts; run_export() adds the
        # planned_bytes() pre-pass for its (done, total) callback.
        range_kwargs = dict(kwargs) if args.no_progress else {**kwargs, "progress": on_bytes}
        if not args.no_progress:
            kwargs["progress"] = progress

        wchar_before = _wchar()
        self_before = resource.getrusage(resource.RUSAGE_SELF)
        children_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        start = time.perf_counter()
        holes = zeros = 0
        sink_wait: float | None = None
        if args.mode in ("default", "inprocess"):
            file_sink = LocalFileSink(dst, staged=False)
            result = await run_sink_export(
                file_sink,
                content.size,
                sparse=kwargs["sparse"],
                body=lambda: content.export_range(file_sink, 0, content.size, **range_kwargs),
            )
            written, size, holes, zeros = result.bytes_written, result.logical_size, result.holes, result.zeros
        elif args.mode == "stream":
            written, size = await _stream_to_file(content, dst)
        elif args.mode == "sink-parent":
            sink = _parent_only_sink(dst, args.sink_mib_s, args.sink_model)
            sink_size = content.size
            await sink.open(sink_size, sparse=not args.dense)
            try:
                result = await content.export_range(sink, 0, content.size, **range_kwargs)
                await sink.commit()
            except BaseException:
                await sink.abort()
                raise
            written, size, holes, zeros = result.bytes_written, result.logical_size, result.holes, result.zeros
        elif args.mode == "segmented":
            segmented = await run_export(
                content,
                _discarding_segmented_sink(args.segment_mib * _MIB, args.buffered_segments, args.storage),
                **kwargs,
            )
            written, size, holes, zeros = (
                segmented.bytes_written,
                segmented.logical_size,
                segmented.holes,
                segmented.zeros,
            )
            sink_wait = segmented.sink_wait_seconds
        elif args.mode in ("plan-only", "count-only"):
            written, size = await _plan_only(content, args.mode, args.window_entries)
        else:
            raise SystemExit(f"unknown mode {args.mode!r}")
        wall = time.perf_counter() - start
        wchar_after = _wchar()
        self_after = resource.getrusage(resource.RUSAGE_SELF)
        children_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    finally:
        await session.close()

    allocated = os.stat(dst).st_blocks * 512 / _MIB if dst != _DEV_NULL and dst.exists() else None
    digest = _file_sha256(dst) if args.hash and dst != _DEV_NULL and dst.exists() else None
    if dst != _DEV_NULL:
        with contextlib.suppress(FileNotFoundError):
            dst.unlink()
    return RunResult(
        mode=args.mode,
        wall_s=wall,
        parent_cpu_s=(self_after.ru_utime + self_after.ru_stime) - (self_before.ru_utime + self_before.ru_stime),
        child_cpu_s=(children_after.ru_utime + children_after.ru_stime)
        - (children_before.ru_utime + children_before.ru_stime),
        parent_peak_rss_mib=_rss_mib(self_after.ru_maxrss),
        child_peak_rss_mib=_rss_mib(children_after.ru_maxrss),
        bytes_written=written,
        logical_size=size,
        holes=holes,
        zeros=zeros,
        sha256=digest,
        store_calls=_count_calls(calls) if calls is not None else None,
        allocated_mib=allocated,
        wchar_mib=(wchar_after - wchar_before) / _MIB if wchar_before is not None and wchar_after is not None else None,
        sink_wait_s=sink_wait,
    )


async def _plan_only(content: Any, mode: str, window_entries: int | None) -> tuple[int, int]:
    from synology_apm_repo.sdk.dedup.chunk_walk import DEFAULT_WINDOW_ENTRIES, count_planned_bytes, plan_chunks_windowed

    base, start, size = content.export_window()
    if mode == "count-only":
        return await count_planned_bytes(base, start, start + size), size
    planned = 0

    async def _discard(offset: int, length: int) -> None:
        return None

    async for plan in plan_chunks_windowed(
        base, start, start + size, start, write_zero_fill=_discard, max_entries=window_entries or DEFAULT_WINDOW_ENTRIES
    ):
        planned += sum(len(runs) for runs in plan.groups.values())
    return planned, size


def _add_run_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repo", default="", help="repository path (local); or use --sample")
    parser.add_argument("--sample", default="", help="name of a [[remote_storage]] entry in smoke_samples.toml")
    parser.add_argument(
        "--ref", required=True, help="canonical node ref after the '#', e.g. cat:3/wl:3/ver:.../object:1"
    )
    parser.add_argument("--mode", choices=MODES, default="default")
    parser.add_argument("--workdir", default=tempfile.gettempdir(), help="where the (sparse) output file goes")
    parser.add_argument("--window-entries", type=int, default=0, help="override DEFAULT_WINDOW_ENTRIES (chunks)")
    parser.add_argument("--workers", type=int, default=0, help="override default_worker_count()")
    parser.add_argument("--view-mib", type=int, default=0, help="export only this many MiB (a small export)")
    parser.add_argument("--view-offset-mib", type=int, default=0)
    parser.add_argument(
        "--count-calls",
        action="store_true",
        help="count this process's ObjectStore calls (workers' own calls are not seen: use with inprocess)",
    )
    parser.add_argument(
        "--dev-null", action="store_true", help="write to /dev/null: measures the read/decode side, no disk"
    )
    parser.add_argument(
        "--sink-mib-s",
        type=float,
        default=0.0,
        help="sink-parent mode: cap the sink at this many MiB/s (slow disk API)",
    )
    parser.add_argument("--sink-model", choices=("queued", "inline"), default="queued")
    parser.add_argument("--dense", action="store_true", help="sparse=False (writes every zero)")
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="pass no progress callback",
    )
    parser.add_argument("--segment-mib", type=int, default=64, help="segmented mode: MiB per segment")
    parser.add_argument("--buffered-segments", type=int, default=2, help="segmented mode: segments held at once")
    parser.add_argument(
        "--storage", choices=("memory", "spool"), default="memory", help="segmented mode: where segments are held"
    )
    parser.add_argument("--hash", action="store_true", help="also SHA-256 the output (correctness, slow)")


def _run_command(args: argparse.Namespace) -> int:
    result = asyncio.run(_measure(args))
    print(json.dumps(dataclasses.asdict(result) | {"throughput_mib_s": result.throughput_mib_s}))
    return 0


def _matrix_command(args: argparse.Namespace) -> int:
    child_args = [
        sys.executable,
        str(Path(__file__).resolve()),
        "run",
        "--repo",
        args.repo,
        "--sample",
        args.sample,
        "--ref",
        args.ref,
        "--mode",
        args.mode,
        "--workdir",
        args.workdir,
        "--window-entries",
        str(args.window_entries),
        "--workers",
        str(args.workers),
        "--view-mib",
        str(args.view_mib),
        "--view-offset-mib",
        str(args.view_offset_mib),
        "--sink-mib-s",
        str(args.sink_mib_s),
        "--sink-model",
        args.sink_model,
        "--segment-mib",
        str(args.segment_mib),
        "--buffered-segments",
        str(args.buffered_segments),
        "--storage",
        args.storage,
    ]
    for flag, enabled in (
        ("--dense", args.dense),
        ("--no-progress", args.no_progress),
        ("--hash", args.hash),
        ("--dev-null", args.dev_null),
        ("--count-calls", args.count_calls),
    ):
        if enabled:
            child_args.append(flag)
    runs: list[dict[str, Any]] = []
    for index in range(args.warmup + args.repeat):
        completed = subprocess.run(child_args, check=True, capture_output=True, text=True)
        record = json.loads(completed.stdout.strip().splitlines()[-1])
        if index >= args.warmup:
            runs.append(record)
    print(_summarize(args, runs))
    return 0


def _summarize(args: argparse.Namespace, runs: list[dict[str, Any]]) -> str:
    def med(key: str) -> float:
        return float(statistics.median(r[key] for r in runs))

    walls = [r["wall_s"] for r in runs]
    first = runs[0]
    hashes = {r["sha256"] for r in runs if r["sha256"]}
    calls = runs[0].get("store_calls")
    label = args.label or args.mode
    return (
        f"{label:<28} wall {med('wall_s'):6.2f}s (min {min(walls):.2f} max {max(walls):.2f})  "
        f"{med('throughput_mib_s'):7.0f} MiB/s  "
        f"cpu parent {med('parent_cpu_s'):5.1f}s child {med('child_cpu_s'):6.1f}s  "
        f"rss parent {med('parent_peak_rss_mib'):6.0f} MiB child-max {med('child_peak_rss_mib'):5.0f} MiB  "
        f"written {first['bytes_written'] / _MIB:8.0f} MiB"
        + (f"  allocated {med('allocated_mib'):.0f} MiB" if first.get("allocated_mib") is not None else "")
        + (f"  write syscalls {med('wchar_mib'):.0f} MiB" if first.get("wchar_mib") is not None else "")
        + (
            "  store calls " + ", ".join(f"{m}: {int(n)}x {sec:.2f}s" for m, (n, sec) in sorted(calls.items()))
            if calls
            else ""
        )
        + (
            f"  sha256 {next(iter(hashes))[:12]}"
            if len(hashes) == 1
            else (f"  sha256 MISMATCH {hashes}" if hashes else "")
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="one measured export, JSON on stdout")
    _add_run_options(run)
    run.set_defaults(handler=_run_command)
    matrix = sub.add_parser("matrix", help="repeat `run` in fresh processes and print medians")
    _add_run_options(matrix)
    matrix.add_argument("--repeat", type=int, default=5)
    matrix.add_argument("--warmup", type=int, default=1)
    matrix.add_argument("--label", default="", help="row label (defaults to the mode)")
    matrix.set_defaults(handler=_matrix_command)
    args = parser.parse_args(argv)
    handler: Callable[[argparse.Namespace], int] = args.handler
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
