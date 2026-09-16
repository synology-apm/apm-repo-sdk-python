# Real-sample smoke tests -- shared conventions

Three separate, non-pytest tools -- `sdk/`, `cli/`, `browser/`, one per
distribution -- driven against real, on-disk sample repositories
configured in `smoke_samples.toml` (a separate mechanism from
`tests/integration/`'s `--record-against=local:<path>`), run by hand
via `make smoke-test`, never by `make test`/CI. Modeled on
`../apm-sdk-python/tests/smoke/`'s own live-smoke-test design; see that
project's `tests/smoke/README.md` for the shared conventions each of
these adapts (`SmokeContext.call`/`.check`, a rendered `index.md`, a
non-pytest `python -m` entry point) -- the difference is the data source:
a `smoke_samples.toml`-configured set of real on-disk repositories here,
not a live server there.

## Why this exists

Neither `tests/unit/` nor `tests/integration/` ever touches a real sample
tree at run time (see `tests/CLAUDE.md`) -- everything there is synthetic
or replayed `RecordingStore`/`ReplayStore` cassettes. These tools exist to
actually exercise real, production-shaped bytes end to end, on demand,
which neither of those layers is designed to do.

## The three tools

| Tool | Drives | What it's for |
|---|---|---|
| `sdk/` | `synology_apm_repo.sdk`'s public async API, in-process | decode/browse correctness against real bytes: discover -> enumerate -> browse -> read/export, for every workload type a sample has |
| `cli/` | The real, installed `synology-apm-repo-cli` console script, as a subprocess | argv parsing, output rendering, exit codes, config precedence -- the process boundary itself |
| `browser/` | The real `ApmRepoBrowserApp`, via Textual's `App.run_test()` | real screen navigation, keybindings, worker/background-job behavior, diagnostic-mode toggling |

`cli/` and `browser/` deliberately don't re-derive `sdk/`'s own decode
correctness -- see each one's own README for exactly where that line is
drawn. `sdk/` is the one place any of the three does real correctness
checking against real bytes; the other two check that their own layer
*reaches* and *renders* that data correctly, not that the data itself is
right.

## Running them

```
uv run python -m tests.smoke.sdk [--group ...]
uv run python -m tests.smoke.cli [--group ...]
uv run python -m tests.smoke.browser [--group ...]
```

or `make smoke-test` (all three, in that order; not part of `make test`/
CI). Needs `smoke_samples.toml` configured first: copy
`smoke_samples.toml.example` and fill in whichever of its three kinds of
block you have real prerequisites for -- shared by all three tools, since
they point at the same real repositories/backends:

- **`[[local]]`** -- a real, on-disk repository root (`path`), same as
  before.
- **`[[profile]]`** -- a saved `sdk.profiles` connection (`profile`, a
  name created via `synology-apm-repo-cli profile add`), resolved via
  config file + OS keyring at discovery time -- the same mechanism the
  CLI's `--profile` flag and the TUI's `ConnectDialog` profile picker use.
- **`[[remote_storage]]`** -- a live S3/Azure/SMB target built straight
  from credentials in this file (`type = "s3"|"azure"|"smb"` plus that
  backend's own fields), no saved profile involved. Not reopenable by a
  fresh `cli/` subprocess (the real CLI has no raw-credential flag) --
  `sdk/` and `browser/` cover it fully; see `cli/README.md`.

Set each encrypted sample's `key =` field (accepted on every kind) to its
own encryption key. With no `smoke_samples.toml` (or an empty one), a tool
still runs and produces a clean, all-skipped report rather than erroring --
see `_samples.py`'s `load_smoke_samples()`.

## Shared machinery (this directory)

- **`_samples.py`** -- `smoke_samples.toml` loader, shared by all three.
- **`_report.py`** -- `index.md` renderer (`make_report_dir`/
  `write_index`), shared by all three; each tool passes its own `title`/
  `domains`.
- **`_context.py`** -- generic step/result bookkeeping (`DomainStats`,
  `StepResult`, `step_slug`, `to_jsonable`, `_truncate`) every tool's own
  `<tool>/_context.py` builds its own `SmokeContext` around.
- **`_shared_refs.py`** -- real-sample discovery (`discover_repos`) and
  ref-selection (`pick_workload_with_retry`/`find_leaf`/
  `list_representative_refs`), shared across all three -- `cli/`'s and
  `browser/`'s own bootstraps call `list_representative_refs()` directly
  rather than each reimplementing the "discover -> enumerate -> pick one
  leaf per (sample, workload type)" walk `sdk/`'s own, richer bootstrap
  needs a fuller version of anyway.

## Reports

Each tool writes to its own `reports/<tool>/<UTC timestamp>/`:
`index.md` (run metadata, per-domain stats table, full checklist linking
into each domain's detail file), one `<domain>.md` per domain, plus
whatever trace artifact that tool's own README documents.

**`reports/` is gitignored and must stay that way.** Real workload/
connection/item names, and any real content bytes a run reads (mail
subjects, file names, ...), belong there and nowhere else -- never in
anything committed. If a sample ever surfaces something that looks like
real customer data rather than test-account data, treat that as a real
incident, not routine sample content, the same standard
`CONTRIBUTING.md`'s "Sample data" section already holds this project to.

`smoke_samples.toml.example` deliberately carries no notes on what any
specific named sample contains -- that's real-sample detail, kept only in
your own gitignored `smoke_samples.toml`, as a comment on the `[[sample]]`
block it describes.

## How to extend

See each tool's own README for its domain list and "how to extend" detail
specific to it. Across all three: file naming leads with `_`
(`_context.py`, `phases/_<domain>.py`, ...) so pytest's default
`test_*.py` collector ignores this whole tree; internal modules import
from each other freely (this tree is explicitly exempt from
`tests/CLAUDE.md`'s "no test module imports another" rule -- that rule is
about the flat `tests/unit`/`tests/integration` pytest tree, not this
self-contained set of tools).

## After any change

`uv run mypy` / `uv run ruff check .` (i.e. plain `make test`) already
cover `tests/smoke/` -- `[tool.mypy]`'s `files` list and
`[tool.ruff.lint.per-file-ignores]`'s `"tests/**"` entry in
`pyproject.toml` both apply here already, no separate invocation needed.
`make test` itself never runs any of these three tools (no `test_*`
function exists anywhere in this tree for pytest to collect) -- re-run
`make smoke-test` by hand after a change, against whatever samples you
have configured.
