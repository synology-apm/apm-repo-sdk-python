# Browser smoke test -- real-sample conventions

Drives the real `ApmRepoBrowserApp` in-process via Textual's own
`App.run_test()` -- the same harness `tests/unit/browser`/
`tests/integration/browser` use -- pointed at real, on-disk sample
repositories instead of a fake `ContentSource`/replayed fixture. Not a
re-derivation of decode/browse correctness (`tests/smoke/sdk/` already
covers that end to end) -- scoped to what's genuinely TUI-only: real
screen navigation, keybindings, worker/background-job behavior, and
diagnostic-mode toggling.

## Purpose and relationship to other test layers

| Layer | Drives | Data source | Offline? |
|---|---|---|---|
| `tests/unit/browser/` | `App.run_test()`, real event loop | fake `ContentSource`/`Session` | yes |
| `tests/integration/browser/` | `App.run_test()`, real event loop | `tests/fixtures/*.json.gz` cassettes | yes (replay) |
| `tests/smoke/sdk/` | `synology_apm_repo.sdk`'s public API, in-process | real repositories | no |
| `tests/smoke/browser/` (this tool) | `ApmRepoBrowserApp`, `App.run_test()` | real repositories | no |

## Out of scope (redundant with other layers)

- Which connections/workloads/versions exist, decode correctness, real
  content bytes -- `tests/smoke/sdk/` already exercises this end to end.
- `--no-sparse-export`'s argv parsing (`test_browser_app.py` covers
  `_parse_args()`) and the sparse-write mechanism itself
  (`test_browser_screens_export_screen.py`, `test_dedup_dedup_file.py`) --
  `phases/_export_worklist.py` runs a single pass at the default
  `default_sparse=True`, checking the background-job plumbing instead.

## Running it

```
uv run python -m tests.smoke.browser [--group navigate|diagnostics_and_verbose|export_worklist|export_folder|hex_preview|key_dialog|remote_connect|help_screen]
```

or `make smoke-test`; needs `../smoke_samples.toml` configured (see
`../README.md`).

## Domains

- **`navigate`** -- real `ConnectDialog` -> local sample path ->
  `BrowseScreen`'s repository/connection tree populates -> goto (`g`) straight to
  a picked, real leaf -> lands on `UnitScreen` with the cursor on it.
- **`diagnostics_and_verbose`** -- `DiagnosticsScreen`'s real `verify()`
  findings render without crashing; `d`'s verbose-mode toggle actually
  propagates.
- **`export_worklist`** -- a real background export end to end:
  `ExportScreen` -> backgrounded (`b`) -> `WorklistScreen` lists it ->
  real file lands on disk. Skips the worklist-specific check (not a
  failure) when a small real leaf finishes exporting before it could be
  backgrounded.
- **`export_folder`** -- `e` on the folder holding the leaf `navigate` landed on
  exports everything below it through the real `ExportScreen` and worker:
  the job finishes without error, no `.part` file is left, and every planned
  file is on disk with its planned size. Needs `navigate` in the same run;
  a run past 300 s is cancelled and reported as skipped, not failed.
- **`hex_preview`** -- `HexPreviewScreen` (`x`, diagnostic-mode only) on
  the same picked leaf, checking the dump *renders* in the expected
  offset/hex/ASCII shape -- not re-deriving byte correctness.
- **`key_dialog`** -- for one configured encrypted, unambiguous sample:
  `ConnectDialog` (no key at connect time) -> entering a connection
  triggers `KeyDialog` automatically -> paste the real key -> confirms
  unlocked. Runs in its own, separate `App.run_test()` session (see
  `__main__.py`) so this connect never disturbs the main session's
  already-connected repository.
- **`help_screen`** -- `?` opens the real `HelpScreen`, listing at least
  one binding from the active screen's real `active_bindings`.
- **`remote_connect`** -- for every configured `[[profile]]`/
  `[[remote_storage]]` sample: `ConnectDialog`'s real S3/Azure/SMB tab,
  either through the saved-profile picker (`[[profile]]`) or by filling the
  raw fields directly (`[[remote_storage]]`) -- the one place any of this
  project's tests exercises those tabs against a real backend. Not a
  re-derivation of `navigate`'s own goto/browse coverage -- this domain
  stops at "the repository node appeared," the same scope `navigate`'s own
  `.connect` step has for a local sample.

## Session shape

Every domain but `key_dialog`/`remote_connect` shares one `App.run_test()`
session and one connected repository (`ctx.data["main_ref"]`). It is picked
from `[[local]]` samples only, since these domains drive `ConnectDialog`
via `connect_local()`, which takes a filesystem path, and prefers an
unencrypted sample so key-unlocking stays `key_dialog`'s job. `key_dialog`
opens its own session against `ctx.data["encrypted_ref"]` (also
`[[local]]`-only). Both come from `../_shared_refs.py`'s
`list_representative_refs` (one representative, real leaf per (sample,
workload type), preferring a readable repository) and `pick_session_refs`.

`remote_connect` opens one fresh session *per* configured `[[profile]]`/
`[[remote_storage]]` sample (`ctx.data["remote_entries"]`): connecting a
second source in the same session replaces the first's tree (a new scan's
`RescanStarted` clears the previous repositories, `core/browse/update.py`).
It drives `ConnectDialog` from each sample's own config, since neither
kind's ref is a filesystem path `connect_local()` could use.

## How to extend

1. **Add a check to an existing domain** -- follow that domain's own
   `phases/_<domain>.py` step-naming convention.
2. **Add a new domain** -- create `phases/_<domain>.py`; register it in
   `DOMAINS` (`_context.py`) and `_ORDER` (`__main__.py`), plus
   `_MAIN_SESSION_PHASES` or, if it needs its own `App.run_test()`
   session the way `key_dialog`/`remote_connect` do (see "Session shape"
   above), `_OWN_SESSION_PHASES` and its own session block in `_run()`;
   add its coverage expectations to the relevant sample(s)' comments in
   your `smoke_samples.toml`.
3. **A new named sample surfaces** -- see `tests/smoke/sdk/README.md`'s
   "How to extend" for the shared `smoke_samples.toml.example` recipe;
   nothing about it is browser-specific.
