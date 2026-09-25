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

or `make smoke-test` (runs `sdk`/`cli`/`browser` together; not part of
`make test`/CI -- see the `Makefile`). Needs `../smoke_samples.toml`
configured -- see `../README.md`.

`--group` runs one domain only; each repository's own discovery and
catalog enumeration (`__main__.py`'s `_process_entry`/`_enumerate_catalog`)
still always run for it regardless, since it's cheap, metadata-only I/O --
no reason to gate something this cheap behind `--group`.

## Report

`../reports/sdk/<UTC timestamp>/` (see `../README.md` for the shared shape) --
this tool additionally writes `store_trace.jsonl`: one line per
underlying `ObjectStore` call, tagged with the sample it came from, via
`Session.discover`'s existing `trace=` callback (`api/session.py`) -- not
a mechanism this tool reinvents.

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
  (the `CatalogId` alongside each entry matters: a repository can hold more
  than one `Catalog`, and `Catalog.provider()` doesn't validate that a
  `Version` belongs to it, so a domain re-deriving "some catalog" from
  `ri.repo.catalogs()[0]` instead of resolving this `CatalogId` back to
  its live `Catalog` -- via `_shared_refs.py`'s `resolve_catalog`, which
  wraps the real `Repository.catalog_by_id()`, always freshly, never a
  cached `Catalog` object -- would silently build a provider against the
  wrong or a stale one: a key change tears down and reopens every
  `DedupRepo` under the hood, so a `Catalog` cached from before that
  change would end up pointing at a closed one), and
  `bootstrap_elapsed: dict[str, float]` (wall-clock seconds per sample's
  own catalog enumeration, keyed by `RepoInfo.sample_name` --
  `catalog.py`'s own enumeration-budget check reads it) -- both extended
  one repository's own entries at a time, by `__main__.py`'s
  `_enumerate_catalog`, immediately before that repository's own turn
  running every selected domain's `run_for_repo`, not populated once for
  every sample up front -- see `.._shared_refs.py`'s `RepoInfo` docstring
  for exactly what each field means and why `sample_name` is
  index-suffixed. There is no `repos` entry -- nothing reads a repository
  after its own turn, so there's nothing to keep a shared list of.
- **Per-sample, not per-type-globally**: `device`/`fs`/`saas` each
  exercise one representative workload per `(sample, workload type)` (or
  `(sample, sub_type)` for `saas`) pair found, not just one globally --
  the point of running against several differently-shaped samples is that
  the *same* workload type behaves differently per sample (encrypted vs
  not, object-store vs local, metadata-only vs healthy); collapsing to one
  global example per type would hide most of that. This extends to the
  skip decisions too: a domain lacking its workload type in one sample is
  a skip recorded for *that sample*, not a once-per-run aggregate decided
  from every sample's data up front.
- **`degrade_on`**: deliberately narrow -- only true data-gap errors
  (`NotFoundError`/`DataCorruptError`/`ChunkCompactedError`/`UnsupportedDataFormatError`).
  `KeyRequiredError`/`KeyMismatchError` are never caught this way: a repository lacking a
  verified key is known *before* any read is attempted (`RepoInfo.
  readable`, computed at bootstrap) and skipped proactively with an
  accurate reason instead.
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
   kind (`[[local]]`/`[[profile]]`/`[[remote_storage]]` -- see
   `../README.md`) to `../smoke_samples.toml.example`, following
   `tests/CLAUDE.md`'s sample coverage table; note what it's expected
   to cover and any expected skips/degradations only in your own
   `smoke_samples.toml`'s comment on that block.
