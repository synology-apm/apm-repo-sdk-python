# Real-sample smoke tests -- shared conventions

Three non-pytest tools -- `sdk/`, `cli/`, `browser/`, one per
distribution -- run by hand (`make smoke-test`, never `make test`/CI)
against real sample repositories (local, saved-profile, or live remote
storage) configured in `smoke_samples.toml`. The pytest suite never touches
a real sample at run time (see `tests/CLAUDE.md`); these tools exercise
real bytes end to end, on demand. Each tool has a `SmokeContext` that
records every step (`check`/`skip`, plus `call` in the in-process
`sdk/`/`browser/` tools and `run` in `cli/`), a rendered `index.md`, and a
`python -m` entry point.

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

or `make smoke-test` (all three, in that order). Needs
`smoke_samples.toml` configured first: copy `smoke_samples.toml.example`
and fill in whichever of its three kinds of block you have real
prerequisites for -- shared by all three tools, since they point at the
same real repositories/backends:

- **`[[local]]`** -- a real, on-disk repository root (`path`).
- **`[[profile]]`** -- a saved `sdk.profiles` connection (`profile`, a
  name created via `synology-apm-repo-cli profile add`), resolved via
  config file + OS keyring at discovery time -- the same mechanism the
  CLI's `--profile` flag and the TUI's `ConnectDialog` profile picker use.
- **`[[remote_storage]]`** -- a live S3/Azure/SMB target built straight
  from credentials in this file (`type = "s3"|"azure"|"smb"` plus that
  backend's own fields), no saved profile involved. The real CLI has no
  raw-credential flag, so `cli/` saves a throwaway profile for it for the
  run's duration; see `cli/README.md`.

Set each encrypted sample's `key =` field (accepted on every kind) to its
own encryption key. With no `smoke_samples.toml` (or an empty one), a tool
still runs and produces a clean, all-skipped report (`_samples.py`'s
`load_smoke_samples()`).

## Shared machinery (this directory)

- **`_samples.py`** -- `smoke_samples.toml` loader, shared by all three.
- **`_report.py`** -- `index.md` renderer (`make_report_dir`/
  `write_index`), shared by all three; each tool passes its own `title`/
  `domains`.
- **`_context.py`** -- step/result bookkeeping: `ReportContext` (report
  files, stats, `check`/`skip`) and `CallContext` (adds `call`), which each
  tool's `<tool>/_context.py` subclasses as its `SmokeContext`.
- **`_shared_refs.py`** -- real-sample discovery (`discover_repos`) and
  ref selection (`pick_workload_with_retry`/`find_leaf` for `sdk/`;
  `list_representative_refs`/`pick_session_refs` for `cli/`'s and
  `browser/`'s bootstrap).
- **`_trace_step.py`** -- the `<domain>.<step>` label a store call belongs
  to (a `ContextVar` that `sdk/`'s `SmokeContext.call` sets), so its
  `store_trace.jsonl` attributes each call to the step that issued it.
- **`_file_keyring.py`** -- a file-backed `keyring` backend, so `cli/`'s
  throwaway profiles keep their secrets in a temp file instead of the OS
  keychain.

## Reports

Each tool writes to its own `reports/<tool>/<UTC timestamp>/`:
`index.md` (run metadata, per-domain stats table, resource usage -- CPU and max
RSS from `_report.resource_usage` -- and the full checklist linking into each
domain's detail file), one `<domain>.md` per domain, plus
whatever trace artifact that tool's own README documents.

**`reports/` is gitignored and stays that way**: real workload/
connection/item names and any real content a run reads (mail subjects,
file names, ...) stay there, and notes on what a specific sample holds go
in your own gitignored `smoke_samples.toml` (see its template's header).
Something that looks like real customer data rather than test-account data
is handled as `CONTRIBUTING.md`'s "Sample data" section says.

## How to extend

See each tool's own README for its domain list and "how to extend" detail
specific to it. Across all three: file names lead with `_` (`_context.py`,
`phases/_<domain>.py`, ...), so pytest collects nothing here; modules
import each other freely by relative import (`tests/CLAUDE.md`'s
shared-code placement rules cover the pytest tree, not these tools).

## After any change

`make lint` (part of `make test`) already type-checks and lints
`tests/smoke/`, but nothing runs these tools for you -- re-run
`make smoke-test` by hand after a change, against whatever samples you
have configured.
