"""``synology-apm-repo-cli dump bucket|composition|chunkmap <path>`` — raw format-level
inspection of one physical file.

Each subcommand takes a path to one ``.buk`` or composition sub-file, not a
``NodeRef``, and never goes through the catalog. The parsing lives in
``sdk.diagnostics``; this module only presents it.

Without ``--profile``, ``path`` is a local filesystem path; with it, a
store-relative sub-path instead — a ``.<N>`` sequence-id suffix
(FORMAT-SPEC.md: Sequence-id suffix mechanism) is never resolved from a logical name
in either case, so type the exact on-disk filename.
"""

from __future__ import annotations

import contextlib
import dataclasses
import functools
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Annotated, Protocol

import typer

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.consoles import console
from synology_apm_repo.cli.errors import fail_from_apm_error
from synology_apm_repo.cli.options import ProfileOption
from synology_apm_repo.cli.paging import render
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import (
    DUMP_BUCKET_CHUNK_HELP,
    DUMP_BUCKET_PATH_HELP,
    DUMP_CHUNKMAP_OFFSET_HELP,
    DUMP_CHUNKMAP_VERIFY_MAP_HELP,
    DUMP_COMPOSITION_PATH_HELP,
    DUMP_LIMIT_ENTRIES_HELP,
    DUMP_LIMIT_RECORDS_HELP,
    DUMP_OFFSET_WALK_HELP,
    DUMP_VERIFY_MAP_HELP,
)
from synology_apm_repo.cli.trace_render import build_trace_callback
from synology_apm_repo.sdk import ApmRepoError, ObjectStore, TracingStore
from synology_apm_repo.sdk.diagnostics import (
    BucketInspection,
    ChunkMapInspection,
    CompositionWalk,
    inspect_bucket,
    inspect_chunk_map,
    open_local,
    walk_composition,
)
from synology_apm_repo.sdk.presentation import safe
from synology_apm_repo.sdk.profiles import store_from_profile

app = typer.Typer(help="Raw format-level dump of one physical file.")


_DEFAULT_RECORD_LIMIT = 10
_DEFAULT_ENTRY_LIMIT = 50


class _HasPath(Protocol):
    """A frozen dataclass result with a ``path`` field, as
    ``_run_dump``'s ``dataclasses.replace(result, path=path)`` needs."""

    @property
    def path(self) -> str: ...


def _badge(ok: bool, fail_word: str = "FAIL") -> str:
    """Rich markup for one check result: green ``OK`` or red ``fail_word``."""
    return "[green]OK[/green]" if ok else f"[red]{fail_word}[/red]"


@contextlib.asynccontextmanager
async def _resolved_store(path: str, profile: str | None, *, state: CliState) -> AsyncIterator[tuple[ObjectStore, str]]:
    """Resolve ``path``/``profile`` into an ``(ObjectStore, rel)`` pair,
    traced when ``--trace`` is given. An
    ``ApmRepoError`` raised while resolving or reading through the store is
    the CLI error exit; the store is closed here, since no ``Session`` owns
    it."""
    store: ObjectStore | None = None
    try:
        if profile is None:
            store, rel = open_local(Path(path))
        else:
            store = await store_from_profile(profile)
            rel = path
        if (trace := build_trace_callback(state)) is not None:
            store = TracingStore(store, trace)
        yield store, rel
    except ApmRepoError as exc:
        fail_from_apm_error(exc, state)
    finally:
        if store is not None:
            await store.close()


async def _run_dump[ResultT: _HasPath](
    path: str,
    profile: str | None,
    state: CliState,
    *,
    fetch: Callable[[ObjectStore, str], Awaitable[ResultT]],
    build_json: Callable[[ResultT], object],
    render_human: Callable[[ResultT], None],
) -> None:
    """Shared body of ``bucket``/``composition``/``chunkmap``: resolve the
    store, run one ``sdk.diagnostics`` call via ``fetch``, stamp the result
    with the original ``path`` argument (not the store-relative ``rel``
    read from), then print it through ``paging.render()``; ``build_json``
    returns the ``--json`` payload."""
    async with _resolved_store(path, profile, state=state) as (store, rel):
        result = await fetch(store, rel)
    # mypy can't confirm dataclasses.replace() keeps ResultT's concrete
    # type through the _HasPath protocol bound.
    result = dataclasses.replace(result, path=path)  # type: ignore[type-var]
    render(console, state, json=build_json(result), human=lambda: render_human(result), page=True)


def _build_json(result: BucketInspection | ChunkMapInspection) -> object:
    """``bucket()``'s and ``chunkmap()``'s ``--json`` payload."""
    return dataclasses.asdict(result)


def _render_bucket_human(result: BucketInspection) -> None:
    console.print(f"file           : {result.path}")
    console.print(f"major/minor    : {result.major}/{result.minor}")
    console.print(f"mode           : {result.mode:#04x} ({', '.join(result.mode_flags) or 'none'})")
    console.print(f"chunk_num      : {result.chunk_num}")
    console.print(f"chunk_size_crc : {result.chunk_size_crc:#010x}")
    counts_str = " ".join(f"{k}={v}" for k, v in sorted(result.compress_type_counts.items()))
    console.print(f"sizestore      : {counts_str}")
    if result.size_check_ok is None:
        console.print("expected_size  : n/a (uncompressed layout)")
        console.print(f"actual_size    : {result.actual_size}")
    else:
        console.print(f"expected_size  : {result.expected_size}")
        console.print(f"actual_size    : {result.actual_size}  ({_badge(result.size_check_ok, 'MISMATCH')})")
    if result.chunk is not None:
        c = result.chunk
        console.print(
            f"{f'chunk[{c.index}]':<15}: compress={c.compress_type} stored_len={c.stored_len} "
            f"effective_len={c.effective_len} offset={c.offset} length={c.length}"
        )


@typer_async
async def bucket(
    ctx: typer.Context,
    path: Annotated[str, typer.Argument(help=DUMP_BUCKET_PATH_HELP)],
    chunk: Annotated[int | None, typer.Option("--chunk", help=DUMP_BUCKET_CHUNK_HELP)] = None,
    profile: ProfileOption = None,
) -> None:
    """Dump a .buk file's header, SizeStore summary, and the
    expected_bucket_size self-check."""
    state: CliState = ctx.obj

    async def _fetch(store: ObjectStore, rel: str) -> BucketInspection:
        return await inspect_bucket(store, rel, chunk=chunk)

    await _run_dump(path, profile, state, fetch=_fetch, build_json=_build_json, render_human=_render_bucket_human)


def _build_composition_json(result: CompositionWalk) -> object:
    return {
        "path": result.path,
        "header": {"major": result.header[0], "minor": result.header[1]} if result.header is not None else None,
        "records": [dataclasses.asdict(r) for r in result.records],
    }


def _render_composition_human(result: CompositionWalk, *, limit: int) -> None:
    if result.stopped_with_error is not None:
        console.print(
            f"[dim](stopped walking at offset {result.next_offset}: {safe(str(result.stopped_with_error))})[/dim]"
        )
    console.print(f"file : {result.path}")
    if result.header is not None:
        major, minor = result.header
        console.print(f"header : major={major} minor={minor}")
    for entry in result.records:
        line = (
            f"head_off={entry.head_off:<10} status={entry.status:<10} map_num={entry.map_num:<8} "
            f"mode={entry.mode:#06x} attr_leng={entry.attr_leng}"
        )
        if entry.map_crc_ok is not None:
            map_crc = _badge(entry.map_crc_ok)
            line += f"  map_crc={map_crc}"
        console.print(line)
    if not result.records:
        console.print("[dim](no records)[/dim]")
    elif len(result.records) == limit and result.next_offset < result.file_size:
        console.print(f"[dim](stopped at --limit={limit}; more records follow at offset {result.next_offset})[/dim]")


@typer_async
async def composition(
    ctx: typer.Context,
    path: Annotated[str, typer.Argument(help=DUMP_COMPOSITION_PATH_HELP)],
    offset: Annotated[int | None, typer.Option("--offset", help=DUMP_OFFSET_WALK_HELP)] = None,
    limit: Annotated[int, typer.Option("--limit", help=DUMP_LIMIT_RECORDS_HELP)] = _DEFAULT_RECORD_LIMIT,
    verify_map: Annotated[bool, typer.Option("--verify-map", help=DUMP_VERIFY_MAP_HELP)] = False,
    profile: ProfileOption = None,
) -> None:
    """Walk composition records in PATH, printing each RecordHead — the
    header, if present (subID=0 only), plus up to --limit records
    starting at --offset (default: right after the header, or byte 0 for
    a non-subID=0 file)."""
    state: CliState = ctx.obj

    async def _fetch(store: ObjectStore, rel: str) -> CompositionWalk:
        return await walk_composition(store, rel, offset=offset, limit=limit, verify_map=verify_map)

    await _run_dump(
        path,
        profile,
        state,
        fetch=_fetch,
        build_json=_build_composition_json,
        render_human=functools.partial(_render_composition_human, limit=limit),
    )


def _render_chunkmap_human(result: ChunkMapInspection) -> None:
    console.print(f"file     : {result.path}")
    console.print(f"head_off : {result.head_off}")
    console.print(f"map_num  : {result.map_num}")
    if result.map_crc_ok is not None:
        map_crc = _badge(result.map_crc_ok)
        console.print(f"map_crc  : {map_crc}")
    for item in result.entries:
        addr_str = (
            f"(stream={item.addr.stream_id},bucket={item.addr.bucket_id},chunk={item.addr.chunk_idx})"
            if item.addr
            else "-"
        )
        console.print(
            f"  [{item.index:>4}] {item.kind:<8} file_offset={item.file_offset:<10} "
            f"end={item.end_offset:<10} inherit={item.is_inherit!s:<5} addr={addr_str:<28} "
            f"map_num={item.map_num} repeat={item.repeat}"
        )
    if result.map_num > len(result.entries):
        console.print(
            f"[dim](showing {len(result.entries)} of {result.map_num} entries — pass --limit to see more)[/dim]"
        )


@typer_async
async def chunkmap(
    ctx: typer.Context,
    path: Annotated[str, typer.Argument(help=DUMP_COMPOSITION_PATH_HELP)],
    offset: Annotated[int, typer.Option("--offset", help=DUMP_CHUNKMAP_OFFSET_HELP)],
    limit: Annotated[int, typer.Option("--limit", help=DUMP_LIMIT_ENTRIES_HELP)] = _DEFAULT_ENTRY_LIMIT,
    verify_map: Annotated[bool, typer.Option("--verify-map", help=DUMP_CHUNKMAP_VERIFY_MAP_HELP)] = False,
    profile: ProfileOption = None,
) -> None:
    """Dump the ChunkMapRecord array — the core read structure — of the
    record at --offset: each entry's kind, file/end offsets, inherit
    flag, map number and repeat count, plus (with --verify-map) the
    chunk-map CRC check result."""
    state: CliState = ctx.obj

    async def _fetch(store: ObjectStore, rel: str) -> ChunkMapInspection:
        return await inspect_chunk_map(store, rel, offset, limit=limit, verify=verify_map)

    await _run_dump(path, profile, state, fetch=_fetch, build_json=_build_json, render_human=_render_chunkmap_human)


app.command("bucket")(bucket)
app.command("composition")(composition)
app.command("chunkmap")(chunkmap)
