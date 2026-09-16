"""``synology-apm-repo-cli dump bucket|composition|chunkmap <path>`` — raw format-level
inspection of one physical file.

Each subcommand takes a bare path to one physical file — not a ``NodeRef``
— because these commands exist to answer "what does this specific byte
layout actually say" for one ``.buk`` or composition sub-file, a question
a ``NodeRef`` (which routes through catalog/dispatch) would get in the way
of, not help with. The actual parsing lives in ``sdk.diagnostics``, built
on the same Codec/Dedup Layer parsers the real read path uses — this
module is presentation only.

Without ``--profile``, ``path`` is a literal local filesystem path, as
always. With ``--profile``, it's a store-relative sub-path instead — the
same interpretation shift ``ls``/``tree``/``cat``/``export`` already give
their own ref argument — since a ``.buk``/composition file's byte layout
is identical whether it lives on local disk, S3, Azure, or SMB
(FORMAT-SPEC.md: sequence-id-suffix/composition-splitting); this command
never needed the ``NodeRef``/Catalog machinery locally and doesn't need it
remotely either. A ``.<N>`` sequence-id suffix (FORMAT-SPEC.md:
sequence-id-suffix) is never resolved from a logical name in either
case — type the exact on-disk filename, local or remote.
"""

from __future__ import annotations

import contextlib
import dataclasses
from collections.abc import AsyncIterator
from pathlib import Path

import typer
from rich.console import Console

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.errors import fail, friendly_message, unwrap
from synology_apm_repo.cli.options import ProfileOption
from synology_apm_repo.cli.paging import paged
from synology_apm_repo.cli.profile_store import resolve_profile_store
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import (
    DUMP_BUCKET_CHUNK_HELP,
    DUMP_BUCKET_PATH_HELP,
    DUMP_CHUNKMAP_OFFSET_HELP,
    DUMP_COMPOSITION_PATH_HELP,
    DUMP_LIMIT_ENTRIES_HELP,
    DUMP_LIMIT_RECORDS_HELP,
    DUMP_OFFSET_WALK_HELP,
    DUMP_VERIFY_HELP,
    DUMP_VERIFY_MAP_HELP,
)
from synology_apm_repo.sdk.diagnostics import (
    BucketInspection,
    ChunkMapInspection,
    CompositionWalk,
    inspect_bucket,
    inspect_chunk_map,
    open_local,
    walk_composition,
)
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.storage.base import ObjectStore, aclose_if_possible

app = typer.Typer(help="Raw format-level dump of one physical file.")

console = Console()

_DEFAULT_RECORD_LIMIT = 10
_DEFAULT_ENTRY_LIMIT = 50


def _badge(ok: bool, fail_word: str = "FAIL") -> str:
    """``"[green]OK[/green]"``/``"[red]<fail_word>[/red]"`` — the pass/fail
    rich-markup rendering shared by every check result this command family
    prints (bucket size, chunk-map/RecordHead CRC)."""
    return "[green]OK[/green]" if ok else f"[red]{fail_word}[/red]"


@contextlib.asynccontextmanager
async def _resolved_store(path: str, profile: str | None, *, verbose: bool) -> AsyncIterator[tuple[ObjectStore, str]]:
    """Resolve ``path``/``profile`` into an ``(ObjectStore, rel)`` pair,
    mirroring ``browse.py``'s ``opened_repo`` — but store-level, not
    repository-level, since this command family never opens a ``Session``: an
    ``ApmRepoError`` raised while resolving the store itself (a missing
    local directory, an unknown profile name) gets the same clean
    ``fail()`` exit ``unwrap()`` gives one raised while reading through
    it, rather than an internal-error traceback. The local case has
    nothing to close; the ``--profile`` case closes its own ``ObjectStore``
    here, since — unlike every other ``--profile`` command — there's no
    ``Session`` to do it for us."""
    store: ObjectStore | None = None
    try:
        if profile is None:
            store, rel = open_local(Path(path))
        else:
            store = await resolve_profile_store(profile)
            rel = path
        yield store, rel
    except ApmRepoError as exc:
        fail(friendly_message(exc, verbose=verbose), cause=exc)
    finally:
        if store is not None:
            await aclose_if_possible(store)


def _render_json(result: BucketInspection | ChunkMapInspection) -> None:
    """``console.print_json(data=dataclasses.asdict(result))`` — shared by
    ``bucket()``'s and ``chunkmap()``'s ``--json`` output, both plain
    ``asdict()`` dumps of their own result dataclass.
    ``composition()``'s own ``_render_composition_json`` stays separate:
    it builds a custom dict, not a plain ``asdict()``."""
    console.print_json(data=dataclasses.asdict(result))


def _render_bucket_human(result: BucketInspection) -> None:
    console.print(f"file           : {result.path}")
    console.print(f"major/minor    : {result.major}/{result.minor}")
    console.print(f"mode           : {result.mode:#04x} ({', '.join(result.mode_flags) or 'none'})")
    console.print(f"chunk_num      : {result.chunk_num}")
    console.print(f"chunk_size_crc : {result.chunk_size_crc:#010x}")
    counts_str = " ".join(f"{k}={v}" for k, v in sorted(result.compress_type_counts.items()))
    console.print(f"sizestore      : {counts_str}")
    check = _badge(result.size_check_ok, "MISMATCH")
    console.print(f"expected_size  : {result.expected_size}")
    console.print(f"actual_size    : {result.actual_size}  ({check})")
    if result.chunk is not None:
        c = result.chunk
        console.print(
            f"chunk[{c.index}]      : compress={c.compress_type} stored_len={c.stored_len} "
            f"effective_len={c.effective_len} offset={c.offset} length={c.length}"
        )


@typer_async
async def bucket(
    ctx: typer.Context,
    path: str = typer.Argument(..., help=DUMP_BUCKET_PATH_HELP),
    chunk: int | None = typer.Option(None, "--chunk", help=DUMP_BUCKET_CHUNK_HELP),
    profile: ProfileOption = None,
) -> None:
    """Dump a .buk file's header, SizeStore summary, and the
    expected_bucket_size self-check."""
    state: CliState = ctx.obj
    async with _resolved_store(path, profile, verbose=state.verbose) as (store, rel):
        try:
            result = await unwrap(inspect_bucket(store, rel, chunk=chunk), verbose=state.verbose)
        except IndexError as exc:
            fail(str(exc))
    result = dataclasses.replace(result, path=path)

    if state.json:
        _render_json(result)
    else:
        with paged(console):
            _render_bucket_human(result)


def _render_composition_json(result: CompositionWalk) -> None:
    console.print_json(
        data={
            "path": result.path,
            "header": {"major": result.header[0], "minor": result.header[1]} if result.header is not None else None,
            "records": [dataclasses.asdict(r) for r in result.records],
        }
    )


def _render_composition_human(result: CompositionWalk, *, limit: int) -> None:
    if result.stopped_with_error is not None:
        console.print(f"[dim](stopped walking at offset {result.next_offset}: {result.stopped_with_error})[/dim]")
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
    path: str = typer.Argument(..., help=DUMP_COMPOSITION_PATH_HELP),
    offset: int | None = typer.Option(None, "--offset", help=DUMP_OFFSET_WALK_HELP),
    limit: int = typer.Option(_DEFAULT_RECORD_LIMIT, "--limit", help=DUMP_LIMIT_RECORDS_HELP),
    verify_map: bool = typer.Option(False, "--verify-map", help=DUMP_VERIFY_MAP_HELP),
    profile: ProfileOption = None,
) -> None:
    """Walk composition records in PATH, printing each RecordHead — the
    header, if present (subID=0 only), plus up to --limit records
    starting at --offset (default: right after the header, or byte 0 for
    a non-subID=0 file)."""
    state: CliState = ctx.obj
    async with _resolved_store(path, profile, verbose=state.verbose) as (store, rel):
        result = await unwrap(
            walk_composition(store, rel, offset=offset, limit=limit, verify_map=verify_map), verbose=state.verbose
        )
    result = dataclasses.replace(result, path=path)

    if state.json:
        _render_composition_json(result)
    else:
        with paged(console):
            _render_composition_human(result, limit=limit)


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
    path: str = typer.Argument(..., help=DUMP_COMPOSITION_PATH_HELP),
    offset: int = typer.Option(..., "--offset", help=DUMP_CHUNKMAP_OFFSET_HELP),
    limit: int = typer.Option(_DEFAULT_ENTRY_LIMIT, "--limit", help=DUMP_LIMIT_ENTRIES_HELP),
    verify: bool = typer.Option(False, "--verify", help=DUMP_VERIFY_HELP),
    profile: ProfileOption = None,
) -> None:
    """Dump the ChunkMapRecord array — the core read structure — of the
    record at --offset: each entry's kind, file/end offsets, inherit
    flag, map number and repeat count, plus (with --verify) the
    chunk-map CRC check result."""
    state: CliState = ctx.obj
    async with _resolved_store(path, profile, verbose=state.verbose) as (store, rel):
        result = await unwrap(inspect_chunk_map(store, rel, offset, limit=limit, verify=verify), verbose=state.verbose)
    result = dataclasses.replace(result, path=path)

    if state.json:
        _render_json(result)
    else:
        with paged(console):
            _render_chunkmap_human(result)


app.command("bucket")(bucket)
app.command("composition")(composition)
app.command("chunkmap")(chunkmap)
