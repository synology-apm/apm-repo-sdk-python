# CLI smoke test -- real-sample conventions

Drives the real, installed `synology-apm-repo-cli` console script as a
subprocess (`_cli_runner.py`) against the real sample repositories
configured in `../smoke_samples.toml`. Not a re-derivation of
decode/browse correctness (`tests/smoke/sdk/` already covers that end to
end, in-process) --
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
stdout byte encoding, a real Ctrl-C cancelling a real
`export`.

## Out of scope (redundant with other layers)

- Which connections/workloads/versions exist, decode correctness, real
  content bytes -- `tests/smoke/sdk/` already exercises this end to end;
  `phases/_commands.py` only checks that a command *reaches* and *renders* real
  data, not that the data itself is right.
- Argument-parsing edge cases already covered by
  `tests/unit/cli/test_cli_main.py` and friends (bad flag combinations,
  `--help` text content).
- A manual-only test list: the CLI is a read-only reader with no destructive
  command, so every command this tool drives is safe to fully automate.

## Running it

```
uv run python -m tests.smoke.cli [--group commands|global_flags|export_lifecycle|profile|errors|remote_connect] [--sample NAME ...]
```

or `make smoke-test`; needs `../smoke_samples.toml` configured (see
`../README.md`).

## Domains

- **`commands`** -- one real pass per read-only command (`doctor`, `ls`,
  `tree`, `cat`, `key`, `verify`) against a picked, real ref, both plain
  and `--json`; the root-level `--version`/`-h` flags. `verify` runs at
  `--level quick` only.
- **`global_flags`** -- `--verbose`'s field-visibility gating, `--quiet`
  never suppressing errors, `--progress`'s NDJSON stream (only with
  `--json`), `--trace` never polluting `--json` stdout.
- **`export_lifecycle`** -- a real file export end to end (no leftover
  `.part`, a non-empty final file), plus a real, timed `SIGINT` mid-export:
  a first Ctrl-C is a handled cancellation (`.part` deleted, no final file,
  exit 130, `ExitCode.CANCELLED`).
- **`profile`** -- `profile add/list/show/remove` round trip against a
  sandboxed config dir, driven through `--no-input`'s stdin-secrets flow.
  Uses the `null` keyring backend (`_cli_runner.py`'s `sandboxed_env()`),
  never the real OS keychain.
- **`errors`** -- a bad path and a wrong key, checking exit code 1 and a
  clean error (no leaked SDK method name, no traceback).
- **`remote_connect`** -- one `doctor --profile` per `[[profile]]`/
  `[[remote_storage]]` sample, the counterpart of `browser/`'s
  `remote_connect`. A `[[remote_storage]]` sample has no saved profile, and
  the real CLI reopens a remote target only via `--profile`, so
  `_remote_profiles.py` saves a throwaway profile per sample with the real
  `profile add` and removes it with `profile remove` when the run ends. Its
  config dir and secrets (a file-backed keyring, `../_file_keyring.py`) live
  in a temp dir, never the real `~/.config` or OS keychain.

## Ref selection and shared-bucket siblings

Same as `browser/`: only `[[local]]` samples are bootstrapped, and
`_shared_refs.pick_session_refs()` picks one `main_ref` (unencrypted,
non-SaaS preferred) and one `encrypted_ref`; `commands` runs against those
two, `global_flags`/`export_lifecycle` against `main_ref`, `errors` against
`encrypted_ref`. Remote samples get `remote_connect`'s one check only.
`--sample NAME` (repeatable) restricts a run to the named entries.

`--key` is only ever tried when `RepoInfo.sole_repo_for_entry` is `True` --
the unambiguous case where a sample entry's own config resolves to exactly
one repository, so there's no "which sibling does this key belong to" question to
guess around. In that case the ref used is the *broad*, un-narrowed one
(`RepoInfo.broad_repo_ref`), which accepts `--key` for both vault and
object-store layouts alike.

For a shared bucket's non-sole sibling (two repositories sharing one
bucket), no key is passed and the ref is the narrowed, single-repository
location (`RepoInfo.narrow_repo_ref`): a narrowed rescan's key-verification
probe reports "no repository found" rather than ignoring an unneeded key.
An *encrypted* non-sole sibling therefore can't be re-opened with its key
at all and is skipped at bootstrap (`sdk/` still covers it).

## How to extend

1. **Add a check to an existing domain** -- follow that domain's own
   `phases/_<domain>.py` step-naming convention.
2. **Add a new domain** -- create `phases/_<domain>.py` with an
   `def run(ctx: SmokeContext) -> None`; register it in `DOMAINS`
   (`_context.py`) and `_ORDER`/`_PHASES` (`__main__.py`); add its
   coverage expectations to the relevant sample(s)' comments in your
   `smoke_samples.toml`.
3. **A new named sample surfaces** -- see `tests/smoke/sdk/README.md`'s
   "How to extend" for the shared `smoke_samples.toml.example` recipe;
   nothing about it is CLI-specific.
