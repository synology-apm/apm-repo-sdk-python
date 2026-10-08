# `synology_apm_repo.sdk` — design conventions

See [`ARCHITECTURE.md`](../../../../../ARCHITECTURE.md) (repository root)
for the full layering contract first. This document covers conventions
specific to writing SDK code day to day.

## Design Conventions

Layering, read-only/no-network, cancellation, and the `to_thread()` rule
are in `ARCHITECTURE.md`. Two conventions it leaves to this document:

Everything is `async def` unless a method provably does none of it: a
new method that calls `ObjectStore`, `aiosqlite`, or anything downstream
of either is `async def`; one that's pure computation over
already-fetched data (`DedupFile.view()`, `Repository.key_status`) stays
sync. Anything that walks a potentially-huge sequence
(`DedupFile._extents()`, `CompositionRecord.entries()`,
`iter_repository_layouts()`, `Session.discover()`) is an
`AsyncIterator`/`AsyncGenerator`, never a materialized list — some of
these sequences run to millions of items for a large VM image
(`_extents()` is private — see `chunk_walk.py`'s module docstring
for the one module allowed to call it directly).

Every provider the SDK hands out is a `ClosableUnitProvider`
(`units/base.py`), even one with nothing of its own to release, so a
caller always closes what it got: `async def close()`
plus `__aenter__`/`__aexit__` delegating to it — subclass
`_util/closing.py`'s `AsyncClosing` for the pair, as `SqliteSource`/`DedupRepo`
and every built-in provider do — so a caller writes `async with
await XProvider.create(...) as provider:` instead of a hand-rolled
`try`/`finally`. Use `async with` (not a bare assignment) at every call
site that constructs a provider directly, tests included. A function that
only walks a tree (`find_node`, `plan_tree_export`) takes the narrower,
browse-only `UnitProvider`.

## Docstring Conventions

SDK docstrings exist to survive a real Sphinx build without silently
rotting: `docs/` builds this package's public surface (only — CLI/TUI
docstrings aren't part of it), and a plain Sphinx build only warns on
malformed RST — an unresolvable cross-reference target renders as inert
plain text instead of a link, with no warning at all. `make docs` runs
the nitpicky build (`-n -W`: any warning fails it, locally and in CI's
`docs.yml`; not a bare `sphinx-build`, which also skips the `apidoc` step
`docs/api/`'s gitignored RST stubs depend on), so run it before trusting a new or edited cross-reference.

Every docstring is Google-style, parsed by Sphinx's `napoleon` extension:
a one-line summary, optionally a short rationale/edge-case paragraph,
then `Args:`/`Returns:`/`Raises:` (or `Attributes:` for a
dataclass/`NamedTuple`) only where there's something non-trivial to say
about a parameter, return value, exception, or field — a trivial
one-liner stays a one-liner rather than manufacturing a block that just
restates the signature.

Reference another symbol by its bare name in ` ``double backticks`` `,
not a manual `:class:`/`:meth:`/`:func:`/`:attr:`/`:data:`/`:mod:` role:
`autodoc_typehints = "description"` (`docs/conf.py`) already
cross-references every annotated parameter/return type with zero markup,
so a role on top is a redundant second link that breaks the moment its
target is renamed, moved, or made private. The
one exception is a value object's own summary naming the one function
that constructs it, when the pairing is genuinely stable and one-to-one;
that role must be a full dotted path (`` :class:`~synology_apm_repo.sdk.storage.base.ObjectStore` ``) kept on one unwrapped line — a wrapped
qualified name embeds the newline into the target string, corrupting it.

Cite a `FORMAT-SPEC.md` subsection by its own heading term
(`FORMAT-SPEC.md: repo_info`), not its `§N.M` number, which shifts
silently whenever an earlier section is reordered — a whole chapter
(`§N`, no subsection) is the one exception, since chapters don't reorder.

Napoleon's parser reads a property/attribute docstring's text before its
*first* colon as a type-shorthand cross-reference target (the convention
behind `"""int: The width."""`), so an ordinary sentence like `"""Actual
bytes this chunk occupies: 4096 for ..."""` misparses into a broken
reference to a "class" literally named for that sentence — write the
first sentence colon-free, or push the colon-bearing detail to a second
sentence.

A public class keeps its immediate base public too, or built by
composition instead of inheritance: `autodoc_default_options` has no
`private-members` (so a `_`-prefixed name gets no page of its own
regardless of a docstring pointed at it), but `show-inheritance` prints
a class's base names verbatim, leaking a private base's name onto an
otherwise-public page.

## Adding a New Storage Backend

1. Implement the `ObjectStore` methods (`read`/`size`/`exists`/`listdir`, plus
   `close()`, a no-op if the backend holds nothing), `async def`, in a new
   module under `storage/`; a backend holding clients also extends
   `_util.closing.AsyncClosing` for `async with`.
2. Decide honestly whether the backend has genuine non-blocking I/O (network,
   like `S3Store`/`AzureStore` via `aiohttp`) or needs `asyncio.to_thread()`
   wrapping over a synchronous body (local-file-shaped, like `LocalFsStore`) —
   see `ARCHITECTURE.md`'s async section for which is which and why.
3. Add it to the shared parametrized `ObjectStore` contract tests
   (`tests/unit/sdk/test_storage_object_store_contract.py`) so it's held to the
   same behavior (EOF short-reads, pagination, and every failure mapped to
   `NotFoundError`/`PermissionDeniedError`/`StorageBackendError`) as every
   other backend — deviations must be an explicit, documented, tested
   difference, not an accident.
4. If it needs a third-party dependency, add it as a plain dependency in
   `packages/synology-apm-repo-sdk/pyproject.toml`'s `[project.dependencies]`
   — there are no optional extras in this SDK, everything ships together.
   If that dependency is itself a heavy import (network SDKs like
   `aioboto3`/`azure-storage-blob` are the existing examples), still import
   it lazily inside the module (not at `storage/__init__.py` level, which
   imports every backend module unconditionally) so callers who only use
   `LocalFsStore` don't pay for it.
5. `listdir` returns `Entry(name, size)` sorted by name. Fill `size` from the
   listing request itself when the backend's listing carries it (S3/Azure list
   responses, `scandir` and SMB directory entries all do), so a caller needing
   sizes never issues a `size` per file; `None` for a directory or a size the
   listing lacks. The shared contract tests hold every backend to the same
   entries and sizes.

A capability that doesn't fit any single bucket/container/root — listing
what buckets/containers exist at all, say (`storage/s3.py`'s
`list_buckets()`, `storage/azure.py`'s `list_containers()`) — isn't an
`ObjectStore` method (every one of those is already scoped to one chosen
bucket/container) and doesn't go through a backend's own class. Add it as a
free function beside the class instead, following the same lazy-import
pattern; callers needing it (the TUI's connect dialog, for a "browse
buckets"/"browse containers" action) call the function directly rather than
reaching past it for the underlying client library.

## Adding a Cache

Every cache a `DedupRepo` owns is declared through `cachemanager.py`, so its
bound is visible in one place and `Repository.invalidate_caches()` (the TUI's
`r`) reaches it:

1. Put its bound in `CacheLimits`, with what one entry costs, and create it with
   `self.caches.keyed(name, fetch, maxsize=...)` (or `caches.register(...)` for
   an owner that manages several caches itself, as `Pool` does). A cache with no
   count bound passes `maxsize=None` plus `bounded_by="..."` naming what limits
   it (a closed key set); `keyed` rejects an unexplained one.
2. Register a cache after whatever it depends on: invalidation runs last
   registered first, so a `Table` is dropped before the connection it is bound to
   is closed.
3. A cache whose values need closing (an `aiosqlite` connection) passes
   `on_invalidate` that settles in-flight fetches, closes the values, then
   clears; the default hook only waits for fetches and clears.
4. A cache private to one operation (a verify run's, an export's) is its own
   instance, sized from `CacheLimits`, and is not registered: its lifetime is
   the operation, and sharing it would let a sweep evict interactive entries.
5. Add the new attribute to `TestCacheRegistry` in
   `tests/unit/sdk/test_dedup_repository.py`, which fails on a cache attribute
   nobody decided how to register.

## Adding a New Export Sink

An export's destination is an `ExportSink` (`dedup/export_sink.py`; the local-file
implementation is `dedup/local_file_sink.py`); the
read/decode side never changes. A destination that takes writes at any offset
in any order (a hypervisor disk API, say) subclasses `RandomAccessExportSink`,
usually in its own distribution; it is its own `ExportWriter`, and its single
segment is the whole export.
Import the contract from `synology_apm_repo.sdk.export` (`RandomAccessExportSink`,
`SinkCaps`, `AbortOutcome`, `WorkerTarget`, `SinkDescriptor`, `WorkerWriter`,
`run_export`); `examples/restore_to_nutanix_ahv.py`'s `BlockSink`
and `LibiscsiDescriptor` are a worker-capable reference.

1. Implement `open(logical_size, *, sparse)`, `write_at(offset, data)`,
   `write_zero(offset, length)`, `commit()` and `abort()`, all `async def`,
   plus the `caps` and `preallocated` properties. The lifecycle is the
   `ExportSink` docstring's; the ordering, alignment and `write_zero` rules
   are the `ExportWriter` docstring's. A destination that can't accept writes
   at arbitrary offsets in arbitrary order is not a `RandomAccessExportSink`:
   see "A destination that takes bytes in order only" below.
2. Set `SinkCaps.supports_sparse` honestly: `True` only when a range never
   written reads back as zero. When it is `False`, the scheduler writes
   `HOLE`/`ZERO` ranges as zeros even for a sparse export. `preallocated`
   is `True` only once `open` has reserved the whole destination and it
   reads as zero: a dense export then writes no zero-fill at all (see
   `needs_zero_fill`), and `write_zero` itself must still zero whatever
   range it is given, because a later source can overlap an earlier one's
   data.
3. Worker-process writes are optional. `RandomAccessExportSink`'s own
   `worker_target()` returns `None` and its `note_worker_write()` does
   nothing, so the sink is written in the parent process only: the export
   decodes and writes in-process. To let worker processes write directly,
   override both: `worker_target()`
   returns a `WorkerTarget` whose `SinkDescriptor` is picklable, compares
   equal exactly when two writers reach the same destination, and opens its
   own `WorkerWriter` inside the worker (spawned workers import its class
   by module path, so it must be defined at module level; `None` still
   means in-process, and so does a repository whose store cannot be
   rebuilt in a worker; an `OffsetWriter` composes such a target for you),
   and `note_worker_write()`
   records, before workers are given data to write, that the destination
   may now hold data, so `abort` reports and keeps it.
4. Test it with `tests/unit/sdk/test_dedup_export_sink.py`'s recording-sink
   shape (lifecycle order, abort on failure/cancel) and
   `test_dedup_export_scheduler.py`'s `_MemorySink` shape (a full export
   into the sink, byte-compared against the naive export). The fixture
   classes are per-file by convention; copy the one you need.

### A destination that takes bytes in order only

A zip entry, an upload stream or the parts of a multipart upload cannot be
written in the order the exporter produces bytes, so subclass
`BufferedExportSink` (`dedup/buffered_export_sink.py`); `examples/export_disk_to_zip.py`'s
`ZipSink` is a complete reference. It cuts the export into segments of
`segment_size` bytes (a multiple of 4096), takes each in a buffer where writes
may land in any order, and calls your `flush_segment(segment)` once per
completed segment, in order, on a background task while the exporter fills the
next. Implement `create_destination(logical_size, sparse=...)`,
`flush_segment(segment)` (`segment.blocks()` yields views and
`segment.read_all()` a copy; only the copy outlives the hook),
`finalize_destination()` and
`discard_destination()` (release what a failed export left; return whether
something remains).

`max_buffered_segments` (default 2: one filling, one flushing) bounds the
memory to `segment_size * max_buffered_segments` and paces the export: when the
destination is slower, `begin_segment` waits, and `ExportResult.sink_wait_seconds`
says for how long. `storage="memory"` (the default) holds segments in shared
memory; `storage="spool"` in a temporary file in `spool_dir` (the default
temporary directory may itself be memory-backed). Either way worker processes
write into the buffer directly. A failed flush fails the export at the next
write, and `abort` waits for the flush in progress before `discard_destination`.
Choose `segment_size` for the destination's limits (a multipart part's size and
count, say) and for the exporter, which decodes about one segment's data at a
time: very small segments keep few worker processes busy.

## Adding a New SaaS Workload Provider

The shared `SaasWorkloadProvider`/`SaasWorkloadConfig` base and the
`TeamsChatProvider` exception are described in `ARCHITECTURE.md`'s "Unit
Layer" section. A new workload type is a `SaasWorkloadConfig` constant plus
a `content()` function unless its service-DB *location* mechanism differs
from "look up one fixed table name"; only then does it get its own provider
class.

1. Pick the `TreeStrategy` (`units/saas/tree_strategy/`) that matches the
   workload's tree shape in real sample data. A wrong shape produces subtly
   wrong navigation, not an error.
2. Write `content()` against real sample bytes for that workload type:
   service-DB schemas drift across connector versions, so read them through
   `storage/table.py`'s `Table`. Put the byte-producing logic (building an
   EML/ICS/CSV, rendering HTML, ...) in a new `units/content/saas_<name>.py`
   module (see `ARCHITECTURE.md`'s "Content Layer" section);
   `units/saas/<name>.py` keeps only the tree-navigation wiring that calls
   into it.
3. Wrap the config as `open_<name>_provider = make_saas_provider(<NAME>_CONFIG,
   name="open_<name>_provider")` (`units/saas/provider.py`) and add that
   factory to `units/dispatch.py`'s `_SAAS_SUB_TYPE_CANDIDATES`, which
   `saas_provider_for()` tries before falling back to `RawObjectProvider`.

The Device (`units/device.py`) and FS (`units/fs.py`) provider families have
no equivalent "adding a new one" recipe — there is exactly one of each.
SaaS is the one family designed to grow.

## APM Version Compatibility

This SDK decodes an on-disk format, not a versioned API: compatibility is a
matter of which `FORMAT-SPEC.md` sections a repository matches, not a version
number the SDK negotiates. Absorb a schema difference across samples with
`storage/table.py`'s optional columns (`ARCHITECTURE.md`'s "Cross-cutting
shared mechanisms").
