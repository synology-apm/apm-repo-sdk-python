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

No subprocess here (impossible/impractical for a real terminal UI) --
`App.run_test()` is already the sanctioned in-process harness everywhere
else in this package's own tests; this tool just points it at real
content instead of a fake or replayed one.

## Out of scope (redundant with other layers)

- Which connections/workloads/versions exist, decode correctness, real
  content bytes -- `tests/smoke/sdk/` already exercises this end to end.
- `--no-sparse-export`'s own argv parsing -- `test_browser_app.py` covers
  `_parse_args()` directly (a pure function); the sparse-write mechanism
  itself (real disk-block savings) already has controlled-data coverage
  in `test_browser_export_screen.py` and SDK-level `test_dedup_dedup_
  file.py` -- `export_worklist.py` runs a single pass at the default
  `default_sparse=True`, checking the background-job-lifecycle plumbing
  instead (see that phase's own docstring for why re-deriving the sparse
  mechanism here would be redundant).

## Running it

```
uv run python -m tests.smoke.browser [--group navigate|diagnostics_and_verbose|export_worklist|hex_preview|key_dialog|remote_connect|help_screen]
```

or `make smoke-test` (runs `sdk`/`cli`/`browser` together; not part of
`make test`/CI). Needs `../smoke_samples.toml` configured -- see
`tests/smoke/README.md`.

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
session and one connected repository (`ctx.data["main_ref"]`, picked from
`[[local]]` samples only since every one of these domains drives
`ConnectDialog` via `connect_local()`, which takes a filesystem path;
preferring an unencrypted sample so key-unlocking stays `key_dialog`'s own,
deliberate job). `key_dialog` opens its own, separate session against
`ctx.data["encrypted_ref"]` (also `[[local]]`-only, same reason) -- see
`.._shared_refs.list_representative_refs`'s own docstring for how a
representative ref is picked, and `tests/smoke/cli/README.md`'s "Ref
selection and shared-bucket siblings" section for how a shared-bucket
sibling now resolves via `--key` at its own `narrow_repo_ref` like any
other repository. `remote_connect` opens one fresh session
*per* configured `[[profile]]`/`[[remote_storage]]` sample
(`ctx.data["remote_entries"]`), not one shared session looping over all of
them: reconnecting a second source in the same session replaces the
first's tree rather than adding to it (`BrowseScreen._reset_for_new_scan`
clears the previous scan's repositories/tree -- confirmed empirically), so each
sample needs its own session to stay independent, the same reasoning
`key_dialog`'s own separate session already has. It drives `ConnectDialog`
directly from each sample's own config rather than from a
`RepresentativeRef`, since neither kind's ref is a filesystem path
`connect_local()` could use.

## How to extend

1. **Add a check to an existing domain** -- follow that domain's own
   `phases/_<domain>.py` step-naming convention.
2. **Add a new domain** -- create `phases/_<domain>.py`; register it in
   `DOMAINS` (`_context.py`) and `_ORDER` (`__main__.py`), plus
   `_MAIN_SESSION_PHASES` or, if it needs its own `App.run_test()`
   session the way `key_dialog`/`remote_connect` do (see "Session shape"
   above), `_OWN_SESSION_PHASES`; add its coverage expectations to the
   relevant sample(s)' own comments in your `smoke_samples.toml`.
3. **A new named sample surfaces** -- see `tests/smoke/sdk/README.md`'s
   "How to extend" for the shared `smoke_samples.toml.example` recipe;
   nothing about it is browser-specific.
