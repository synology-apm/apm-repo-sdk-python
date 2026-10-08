# SDK smoke test -- real-sample conventions

Drives only `synology_apm_repo.sdk`'s public API (never the CLI or TUI)
against real, on-disk sample repositories, end to end: discover ->
enumerate connections/workloads/versions -> browse a version's tree ->
read (and, size-permitting, export) a meaningful item -- for every
workload type a configured sample actually has, skipping whatever a
sample doesn't have with a stated reason. See `../README.md` for the
shared conventions this tool (and `../cli/`, `../browser/`) build on --
`SmokeContext.call`/`.check`, the rendered `index.md`, the non-pytest
`python -m` entry point, real-sample handling.

## Running it

```
uv run python -m tests.smoke.sdk [--group catalog|device|fs|saas|diagnostics]
```

or `make smoke-test`; needs `../smoke_samples.toml` configured (see
`../README.md`).

`--group` runs one domain only; each repository's discovery and catalog
enumeration (`__main__.py`'s `_process_entry`/`_enumerate_catalog`) always
run, since they are cheap, metadata-only I/O.

## Report

`../reports/sdk/<UTC timestamp>/` (see `../README.md` for the shared shape) --
this tool additionally writes `store_trace.jsonl`: one line per
underlying `ObjectStore` call, tagged with the sample it came from and the
`<domain>.<step>` that issued it (`step`, empty outside any step), via
`Session.discover()`'s `trace=` callback. `started` is the call's epoch start time.
Summarize one with `uv run python scripts/analyze_store_trace.py <path>`:
per-sample calls and store time, repeated calls by path kind, and which
step issued each repeat.

### Reading a report

- A `✗` FAILED anywhere is worth investigating regardless of which sample
  produced it.
- A `−` SKIPPED for a workload type/sub_type a configured sample's own
  comment in your `smoke_samples.toml` says it *should* supply is a
  discovery/dispatch bug in this tool (or the SDK), not an expected gap --
  check the sample is actually configured and spelled correctly before
  assuming otherwise.
- A `◐` DEGRADED is only unremarkable when that sample's comment says
  so; anywhere else, it's worth a closer look at `<domain>.md`'s
  corresponding detail entry.

## Conventions

- **`ctx.data` registry**:
  `workloads: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]]`
  and `bootstrap_elapsed: dict[str, float]` (wall-clock seconds of each
  sample's catalog enumeration, keyed by `RepoInfo.sample_name`, read by
  `phases/_catalog.py`'s enumeration-budget check). `__main__.py`'s
  `_enumerate_catalog` extends both with one repository's entries right
  before that repository's turn through every selected domain; see
  `../_shared_refs.py`'s `RepoInfo` for each field. A domain turns the
  `CatalogId` back into its live `Catalog` with `_shared_refs.py`'s
  `resolve_catalog` (`Repository.catalog_by_id()`), fresh each time:
  `Catalog.provider()` doesn't check that a `Version` belongs to it, and
  `Repository.set_key()` reopens every opened catalog, so a cached or
  guessed `Catalog` would build a provider against the wrong or a closed one.
- **Per-sample, not per-type-globally**: `device`/`fs`/`saas` each
  exercise one representative workload per `(sample, workload type)` (or
  `(sample, sub_type)` for `saas`) pair found, not just one globally --
  the same workload type behaves differently per sample (encrypted vs
  not, object-store vs local, metadata-only vs healthy). Skip decisions
  follow the same rule: a domain lacking its workload type in one sample is
  a skip recorded for *that sample*, not a once-per-run aggregate.
- **`degrade_on`**: deliberately narrow -- only true data-gap errors
  (`NotFoundError`/`DataCorruptError`/`ChunkCompactedError`/`UnsupportedDataFormatError`).
  `KeyRequiredError`/`KeyMismatchError` degrade only the bootstrap's
  `workloads()`/`versions()` enumeration (a sample configured without its
  correct key); no domain phase catches them: a repository lacking a
  verified key is known *before* any read is attempted
  (`RepoInfo.readable`, computed at bootstrap) and skipped proactively
  with an accurate reason instead.
- **Disk usage**: `content.read()`/`.stream()` are pure in-memory per the
  SDK's own contract -- the *only* place this tool ever touches disk is
  `phases/_shared.py`'s `bounded_read_and_export`, which caps the export
  smoke check to content no larger than `EXPORT_SIZE_CAP` (4 MiB) and
  writes into a `tempfile.TemporaryDirectory()` that's cleaned up
  immediately on exit, regardless of outcome.

## How to extend

1. **Add a check to an existing domain** -- follow that domain's own
   `phases/_<domain>.py` step-naming convention
   (`<domain>.<sample>.<detail>`); add a `ctx.data` key to this README's
   registry note above if a later domain needs to reuse the result.
2. **Add a new domain** -- create `phases/_<domain>.py` with an
   `async def run_for_repo(ctx: SmokeContext, ri: RepoInfo) -> None`,
   purely per-repo (see the "Per-sample, not per-type-globally" convention
   above -- no `run_once`/global aggregate skip); register it in
   `DOMAINS` (`_context.py`) and `_ORDER`/`_PHASES` (`__main__.py`); add
   its coverage expectations to the relevant sample(s)' comments in
   your `smoke_samples.toml`.
3. **A new named sample surfaces** -- add a commented block of the right
   kind (`[[local]]`/`[[profile]]`/`[[remote_storage]]`) to
   `../smoke_samples.toml.example`, following that file's header: what the
   sample covers and its expected skips/degradations go in your own
   `smoke_samples.toml`'s comment on that block.
