# CLI smoke test -- real-sample conventions

Drives the real, installed `synology-apm-repo-cli` console script as a
subprocess (`_cli_runner.py`) against the same real, on-disk sample
repositories `tests/smoke/sdk/` does, configured in
`../smoke_samples.toml`. Not a re-derivation of decode/browse correctness
(`tests/smoke/sdk/` already covers that end to end, in-process) --
scoped to what's genuinely CLI-only: argv parsing, output rendering,
exit codes, config precedence, and the process boundary itself
(packaging, real stdout encoding, real signal handling).

## Purpose and relationship to other test layers

| Layer | Drives | Data source | Offline? |
|---|---|---|---|
| `tests/unit/cli/` | `synology_apm_repo.cli.main.app`, in-process via `typer.testing.CliRunner` | synthetic mocks | yes |
| `tests/integration/cli/` | Same in-process `CliRunner` | `tests/fixtures/*.json.gz` cassettes | yes (replay) |
| `tests/smoke/sdk/` | `synology_apm_repo.sdk`'s public API, in-process | real repositories | no |
| `tests/smoke/cli/` (this tool) | The real, installed console script, as a subprocess | real repositories | no |

Neither `tests/unit/cli/` nor `tests/integration/cli/` ever drives the
actual packaged entry point as a real subprocess -- both go through
Typer's own in-process `CliRunner`. This tool exists to catch what only a
real process boundary can: a packaging/entry-point break, `cat`'s real
stdout byte encoding, a real Ctrl-C double-press cancelling a real
`export`.

## Out of scope (redundant with other layers)

- Which connections/workloads/versions exist, decode correctness, real
  content bytes -- `tests/smoke/sdk/` already exercises this end to end;
  `commands.py` only checks that a command *reaches* and *renders* real
  data, not that the data itself is right.
- Argument-parsing edge cases already covered by
  `tests/unit/cli/test_cli_main.py` and friends (bad flag combinations,
  `--help` text content).
- This CLI has no irreversible/destructive command at all (a read-only
  repository reader -- no `retire`/`lock` analog), so unlike the reference
  project's own `tests/smoke/cli/`, there is no `MANUAL_TESTS.md` carve-out
  here: every command this tool drives is safe to fully automate.
- A configured `[[remote_storage]]` sample (direct S3/Azure/SMB credentials,
  no saved profile) -- the real CLI has no raw-credential flag, only
  `--profile <name>` for a saved profile, so a `RemoteStorageSample`-
  derived repository can't be reopened by a fresh subprocess at all.
  `list_representative_refs(..., exclude_unreopenable_by_cli=True)`
  drops it before this tool ever picks a ref; `sdk/` and `browser/` still
  cover it fully. A `[[profile]]` sample has no such gap -- its ref
  carries the profile name, and this tool's commands pass it as
  `--profile <name>` (see `phases/_shared.py`'s `common_args`).

## Running it

```
uv run python -m tests.smoke.cli [--group commands|global_flags|export_lifecycle|profile|errors]
```

or `make smoke-test` (runs `sdk`/`cli`/`browser` together; not part of
`make test`/CI). Needs `../smoke_samples.toml` configured -- see
`tests/smoke/README.md`.

## Domains

- **`commands`** -- one real pass per read-only command (`doctor`, `ls`,
  `tree`, `cat`, `export`, `key`, `verify`, `dump`, `profile`) against a
  picked, real ref, both plain and `--json`; the root-level `--version`/
  `-h` flags; one `verify --level full --progress always` pass against
  real data volume.
- **`global_flags`** -- `--verbose`'s field-visibility gating, `--quiet`
  never suppressing errors, `--progress`'s NDJSON stream (needs `--json`
  too -- plain `--progress` on its own still renders a human progress bar,
  not NDJSON), `--trace` never polluting `--json`
  stdout.
- **`export_lifecycle`** -- a real file export end to end (`.part` ->
  renamed final file), plus a real, timed `SIGINT` mid-export: a
  first-press Ctrl-C is a *clean* cancellation (exit 0, `.part` deleted,
  no final file), not a failure exit -- `export.py` handles the
  cancellation explicitly and returns normally instead of letting it
  crash the process.
- **`profile`** -- `profile add/list/show/remove` round trip against a
  sandboxed config dir, driven through `--no-input`'s stdin-secrets flow.
  Forces `PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring` (see
  `_cli_runner.py`'s `sandboxed_env()`) -- never touches the real OS
  keychain, both for isolation and because it can block indefinitely on a
  GUI authorization prompt in a headless context.
- **`errors`** -- a bad path and a wrong key, checking exit code 1 and a
  clean, single-line error (no leaked internal method name, no raw
  traceback).

## Ref selection and shared-bucket siblings

`--key` is only ever tried when `RepoInfo.sole_repo_for_entry` is `True` --
the unambiguous case where a sample entry's own config resolves to exactly
one repository, so there's no "which sibling does this key belong to" question to
guess around. In that case the ref used is the *broad*, un-narrowed one
(`RepoInfo.broad_repo_ref`), which accepts `--key` for both vault and
object-store layouts alike.

For a shared bucket's non-sole sibling (`sample-1`'s own "two repositories sharing
one bucket" case), no key is passed at all, and the ref falls back to the
narrowed, single-repository location (`RepoInfo.narrow_repo_ref`) instead -- a
narrowed rescan's own key-verification probe reports "no repository found"
rather than gracefully ignoring an unneeded key, even for the sibling that
doesn't actually need it. `sample-1`'s shared-bucket case is exercised both by this domain's own
ref-selection walk (above) and, separately, by `errors`' own dedicated
wrong-key check, which reports a normal `KeyMismatchError` rather than a broken
discovery when it deliberately supplies an incorrect key.

## How to extend

1. **Add a check to an existing domain** -- follow that domain's own
   `phases/_<domain>.py` step-naming convention.
2. **Add a new domain** -- create `phases/_<domain>.py` with an
   `async def run(ctx: SmokeContext) -> None`; register it in `DOMAINS`
   (`_context.py`) and `_ORDER`/`_PHASES` (`__main__.py`); add its
   coverage expectations to the relevant sample(s)' comments in your
   `smoke_samples.toml`.
3. **A new named sample surfaces** -- see `tests/smoke/sdk/README.md`'s
   "How to extend" for the shared `smoke_samples.toml.example` recipe;
   nothing about it is CLI-specific.
