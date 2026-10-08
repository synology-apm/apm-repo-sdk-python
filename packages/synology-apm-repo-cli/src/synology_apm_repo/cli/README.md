# `synology_apm_repo.cli` — design conventions

See [`ARCHITECTURE.md`](../../../../../ARCHITECTURE.md) (repository root)
first — layering, presentation, and the facade contract are all there,
not repeated here. This document covers CLI-specific command
conventions and the "adding a new command" recipe.

`dump` is the CLI's one user of `sdk.diagnostics`: it inspects a raw file
outside any discovered repository, which the catalog-based facade can't do.

## Development Conventions

- **Every command is `async def`, decorated with `@typer_async`
  (`asyncio_support.py`)**, the one place `asyncio.run()` lives, since Typer
  calls each callback synchronously. `export.py` additionally runs its body
  as its own `Task` so Ctrl-C can cancel it.
- **`ls`/`tree`/`cat`/`export` take exactly one `NodeRef` positional argument**
  (human or canonical form) to say which node they act on, rather than a set
  of `--catalog`/`--workload`/`--version`-style flags.
- **`--profile <name>` selects the *store*, not a node inside it**, so it sits
  beside the `NodeRef` argument rather than inside it, like `--key`. Under
  `--profile`, the path portion of `ls`/`tree`/`cat`/`export`'s ref and `dump
  bucket`/`composition`/`chunkmap`'s `path` argument are store-relative.
  `doctor`/`key`/`verify` take `--profile` as an alternative to their `repo`
  argument (`opened_repo_or_profile`); `dump`'s `path` stays required, and
  `dump`, which opens no `Session`, closes the store itself.
- **A script keys `--json` output off stable identifiers, not display
  strings that disambiguation can change**: `doctor` gives each catalog and
  workload its internal id with `display_name` as a separate field;
  `ls`/`tree` print the disambiguated `name`, and under `--ref` add each
  item's `ref` (`ls` also adds a catalog/workload/version row's `id`).
- **Every exit status a command sets is an `errors.ExitCode`** (a usage
  error is Click's own 2), so a script can tell the outcomes apart; the
  package README's "Exit status" table is the user-facing list.
- **Progress goes to stderr, always**, so `synology-apm-repo-cli cat <ref> >
  out.bin` produces clean stdout. `--progress auto|always|never`, the
  `--json` NDJSON progress events and `--trace` are all stderr-only. Rich
  output goes through `consoles.py`'s shared `console` (stdout) and
  `err_console` (stderr).
- **Exports write `<dst>.part` and rename on success**, so a cancelled or
  failed export never leaves something that looks like a complete file.
  `LocalFileSink(staged=True)` owns that, through the SDK's `run_export`,
  the same entry point the TUI's export worker uses.
- **A folder REF exports its items to `-o <dir>`, one file at a time**,
  through the SDK's `plan_tree_export`, `preflight_tree` and
  `run_tree_export`. A name that isn't one safe path component here, or a
  path another item already takes, is skipped and reported and the command
  exits 1; only a file name the SDK synthesizes (a mail's subject plus
  `.eml`) is made safe and numbered on repeat. The first failure stops the
  run and files already finished stay.
- **An item the backup holds only partly** (its `RestorableUnit.degraded`
  is set, e.g. a PC/PS disk with missing fragments, read back as zeros) is
  still exported, with an `incomplete` line on stderr that `--quiet` does
  not hide.
- **`--keep-partial`** governs whether a cancelled or failed export's `.part`
  file is kept; decide this explicitly for any new export-adjacent command.

## Adding a New Command

1. Add a module under `commands/` with an `async def` callback decorated
   with `@typer_async`. Its docstring is its `--help` text; option help goes
   in `strings.py`. Every parameter is declared
   `name: Annotated[T, typer.Option(...)] = default` (no default for a
   required one), and the shared `--key`/`--profile`/`--object-db-id`/`REPO`
   parameters use `options.py`'s aliases.
2. Read the global flags from `ctx.obj` (`state.py`'s `CliState`). Open the
   repository through `repo_session.py` (`opened_repo`,
   `opened_repo_or_profile`, or `open_repo` inside your own `cli_session`) or, for a
   REF that `ls`/`tree` would walk, `browse.py`'s `walked_ref`: these turn an
   `ApmRepoError` into the CLI error exit and wire in `--progress`/`--trace`.
   Get any other progress meter from `progress_render.build_progress_meter`.
3. Print the final result through `paging.render()` (`--json` or human,
   `page=True` when the output scales with item-tree content), escaping
   repository-derived text with `sdk.presentation`'s `safe()`.
4. Register it in `main.py` (`app.command(...)`, or `app.add_typer(...)` for
   a sub-command group).
5. Add unit tests (`tests/unit/cli/test_cli_commands_<command>.py`) and,
   if it's meaningfully different against real data, an integration test
   (`tests/integration/cli/test_cli_commands_<command>.py`, backed by a
   recorded fixture — see [`tests/CLAUDE.md`](../../../../../tests/CLAUDE.md)'s "`RecordingStore`
   / `ReplayStore`" section).
6. If its long-running work should be cancellable, follow `export.py`'s
   "first Ctrl-C cancels cleanly, second forces exit" pattern.

## Commands and Package Layout

Commands live one module per file under `commands/`, registered in
`main.py`, which is also the source of truth for the current command list
and global flags (`--verbose`, `--json`, `--progress`, `--trace`,
`--quiet`/`-q`, `--no-input`, `--version`) — run
`synology-apm-repo-cli <command> --help` rather than consulting a listing
here that could drift from it. Shared infrastructure
(`asyncio_support.py`'s `typer_async`, `browse.py`'s ref parsing and `walked_ref`,
`repo_session.py`'s session and repo-open lifecycle, `naming.py`'s
disambiguation helpers and `ls`/`tree`'s shared `NodeFields`, `consoles.py`,
`errors.py`, `options.py`, `paging.py`, `progress_render.py`, `state.py`,
`strings.py`, `trace_render.py`) sits alongside `commands/` at the package
root.
