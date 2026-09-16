# `synology_apm_repo.cli` — design conventions

See [`ARCHITECTURE.md`](../../../../../ARCHITECTURE.md) (repository root)
first — layering, presentation, and the facade contract are all there,
not repeated here. This document covers CLI-specific command
conventions and the "adding a new command" recipe.

The CLI talks to the SDK's `Session`/`Repository`/`Catalog` facade
(`api/`) — never to `storage`/`dedup`/`catalog`/`units` directly, with one
explicitly-named exception: `dump.py` reaches into `sdk.diagnostics`
directly (see `sdk/api/__init__.py`'s own docstring for why that one's
scoped outside the facade). See `ARCHITECTURE.md` for the facade contract
and `NodeRef` format.

## Development Conventions

- **Every command is `async def`, decorated with `@typer_async`
  (`asyncio_support.py`).** Typer has no native async support, so this
  decorator is the one place `asyncio.run()` lives — never re-add a nested
  `async def _run(): ...; asyncio.run(_run())` wrapper inside a command
  module. `export.py`'s own `_do_export()` inner `Task` (for its
  Ctrl-C-cancellable body) is unrelated to this and stays as-is.
- **`ls`/`tree`/`cat`/`export` take exactly one `NodeRef` positional argument**
  (human or canonical form) — never add a `--catalog`/`--workload`/`--version`
  style mutually-exclusive flag set; that's the problem `NodeRef` exists to
  avoid.
- **`--profile <name>` selects the *store*, not a node inside it** — orthogonal
  to `NodeRef`'s job and not a violation of the rule above (same seam
  `Session.open` vs `open_remote` already draws one layer down; `--key` is
  the same kind of separate option for the same reason). Under `--profile`,
  `ls`/`tree`/`cat`/`export`'s ref argument's path portion, and `dump
  bucket`/`composition`/`chunkmap`'s `path` argument, are both
  store-relative — never a filesystem path. `doctor`/`key`/`verify` each
  take `--profile` as a standalone alternative to a `repo` argument;
  `dump`'s own `path` stays required either way, since `dump` has no
  root-only form. `dump` is also the one command that closes a
  `--profile`-resolved store itself, via `AsyncCloseable`, since — unlike
  `ls`/`tree`/`cat`/`export` — it never opens a `Session`.
- **`--verbose` gates internal identifiers and the `raw/` fallback axis**,
  per `ARCHITECTURE.md`'s Presentation section — purely presentational,
  nothing in this CLI refuses to run based on it.
- **`--json` output keys off stable internal identifiers, with `display_name`
  as a separate field** — never make a script-consumable `--json` field's key
  be a human display string that can change with disambiguation rules.
- **Progress goes to stderr, always** — `synology-apm-repo-cli cat <ref> > out.bin` must
  produce clean stdout. `--progress auto|always|never` and the `--json`
  NDJSON progress-event stream are both stderr-only.
- **Exports write `<dst>.part` and rename on success** — a cancelled or
  crashed export must never leave something that looks like a complete file.
- **`--keep-partial`** governs whether a cancelled export's `.part` file is
  kept or removed; don't add a new mutating/export-adjacent command without
  deciding this explicitly.

## Adding a New Command

1. Add a new module under `commands/`, following the `@typer_async` shape
   above.
2. Give it the same `--json`/`--verbose`/`--progress` global flags as the
   existing commands if it does anything non-trivial or long-running — see
   `progress_render.py` for the shared `ProgressMeter`-driven rendering, which
   the SDK's `presentation/` module backs (CLI and TUI must render identically
   — see `ARCHITECTURE.md`).
3. Register it in `main.py`.
4. Add unit tests (`tests/unit/cli/test_cli_*.py`) and, if it's meaningfully
   different against real data, an integration test
   (`tests/integration/cli/test_cli_*.py`, backed by a recorded fixture —
   see [`tests/CLAUDE.md`](../../../../../tests/CLAUDE.md)'s "RecordingStore
   / ReplayStore" section).
5. If the command's long-running work should be cancellable, confirm SIGINT
   behavior matches the existing "first Ctrl-C cancels cleanly, second forces
   exit" pattern (`export.py`) rather than inventing a new cancellation UX.

## Commands and Package Layout

Commands live one module per file under `commands/`, registered in
`main.py`, which is also the source of truth for the current command list
and global flags (`--verbose`, `--json`, `--progress`, `--trace`,
`--quiet`/`-q`, `--no-input`, `--version`) — run
`synology-apm-repo-cli <command> --help` rather than consulting a listing
here that could drift from it. Shared infrastructure
(`asyncio_support.py`'s `typer_async`, `browse.py`'s `NodeRef`-walking,
`errors.py`, `options.py`, `paging.py`, `profile_store.py`,
`progress_render.py`, `state.py`, `strings.py`,
`trace_render.py`) sits alongside `commands/` at the package root.
