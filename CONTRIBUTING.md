# Contributing

See [`CLAUDE.md`](CLAUDE.md) for the development guide and the "Install From Source"
section of [`README.md`](README.md) for environment setup.

## Commit Convention

```
feat:     new feature
fix:      bug fix
perf:     performance, same behaviour
refactor: restructuring, same behaviour
docs:     documentation
test:     tests
build:    build, lint and check tooling
chore:    configuration, dependency bumps
```

An optional scope names the distribution (`fix(sdk):`, `refactor(browser):`),
and `!` marks a breaking change to the SDK's public surface
(`refactor(sdk)!:`). Body: describe what was actually verified, not just
what changed.

## Committing a subset while other changes are staged

If other files are already `git add`-ed — e.g. leftover from parallel
agent/session work — and you only want to commit a specific subset, commit
just that subset with `git commit -- <paths>`. A bare `git commit` commits
the entire index regardless of what you just staged. If a commit picks up
more than intended, `git reset --soft HEAD~1` restores the prior index state
so you can redo it correctly.

## Sample data

See `tests/CLAUDE.md`'s "`RecordingStore` / `ReplayStore`" section for
what a recorded fixture proves (repository structure, never backed-up
content's own meaning), how to record one, how samples are named by alias
(real names live only in the untracked
`tests/support/recording/targets.toml`), and how a recording is anonymized.

A recording's catalog-layer data (`db/connection_config`, `workload_config`,
`copy_target_version`, `file_map`, `file_meta`, and similar SQLite files) is
anonymized automatically when the recording session ends — the fields are
registered in `tests/support/recording/anonymize_catalog_metadata.py`
(`SENSITIVE_FIELDS` and the path columns beside it), and no real-to-fake
mapping is stored anywhere.

**Fixture storage**: `tests/fixtures/*.json.gz` cassettes are committed
gzip-compressed. Run this one-time local setup to get a readable `git
diff`/`git log -p` on a re-recorded fixture instead of a binary diff:

```
git config diff.gzjson.textconv "gzip -dc"
git config diff.gzjson.cachetextconv true
```

If a future sample ever contains anything that looks like real customer data
rather than test-account data, treat that as a real incident, not routine
sample content — do not commit it into fixtures or docs, and flag it instead.

## Debugging a backend

Neither entry point lets a dependency's logging reach the terminal:
`cli/main.py::main()` and `browser/app.py::main()` each call the shared
`synology_apm_repo.sdk.presentation.logging_setup.configure_logging()`
first thing, which puts a `NullHandler` on the root logger. Without it
`logging` falls back to its last-resort handler, which
writes every WARNING and above to stderr — `smbprotocol` alone emits one
per pooled connection on each session teardown, which for the TUI lands on
the rendered screen and in the user's shell after it exits.

Set `SYNOLOGY_APM_REPO_LOG` to a file path to get all of it written there
instead, at `DEBUG`. This is the only way to see a dependency's own view of
a failing remote backend:

```
SYNOLOGY_APM_REPO_LOG=./apm-repo.log synology-apm-repo-cli ls "/path/to/repository#..."
```

The file is written as UTF-8 rather than the machine's locale encoding, so a
log naming a non-ASCII share or path stays readable when handed to someone
on another platform. Expect it to be large: `smbprotocol` logs at `DEBUG`
per SMB message, so a real repository walk produces a lot of it.

The SDK itself configures nothing and filters nothing — a library has no
business reconfiguring logging for the whole process, and an embedder's own
configuration is what decides where a dependency's records go.

## Before every commit

```
make test
```

See the `Makefile` for exactly what that runs (format check, lint,
version-consistency check, SDK import-boundary check, SDK layer-direction
and import-cycle check, browser layer check, mypy for three platforms in
parallel, then pytest with the 95% coverage gate enforced; cheapest first,
so a lint or boundary slip fails before the long steps) — the whole suite
runs every time, no real external state needed. Coverage
counts branches, not just lines, and pytest fails a test on any warning it
raises (`ResourceWarning` excepted, see `pyproject.toml`). Every
`except Exception` carries a `# noqa: BLE001` beside its reason. The 95% gate
applies uniformly across the SDK, CLI, and TUI, with no package-level
carve-out. Lines that exist only for the type checker or as a broken-invariant
guard (`assert_never(...)`, `raise AssertionError`/`NotImplementedError`,
`if TYPE_CHECKING:`, `if __name__ == "__main__":`) are excluded by
`pyproject.toml`'s `exclude_also`. Anything else excluded carries an
individual, justified `# pragma: no cover`, scoped to the line, in one of
three categories: real process/terminal I/O (e.g. `cli/main.py`'s and
`browser/app.py`'s own `main()`, which call `sys.exit`/open a real terminal
and so can't run under `CliRunner`/`App.run_test()`); a defensive branch for
a state its caller already rules out; or a real coverage.py false
negative, confirmed directly via `sys.settrace` (bypassing coverage.py
entirely) before reaching for the pragma — that
confirmation is what justifies this last category, not a hunch that a line
should already be covered. Cite the reason in the pragma's own inline
comment either way.

> **Note:** the `fail_under = 95` floor is a shared aggregate backstop, not a
> definition of "sufficiently tested" — coverage.py only reports files it
> actually saw executed, so a source file with no test importing it stays
> absent from the report entirely, and a small new command/backend with zero
> direct tests can still pass the gate if the rest of the codebase absorbs
> the drop. Each package README's "Adding a new..." recipe requiring a test
> for new screens/commands/backends is what actually closes that gap, not
> the percentage.

If the change touches a code path `tests/integration/` replays real bytes
against, re-record the affected fixture by hand with the `make record-fixture`
command `PYTHONPATH=tests uv run python -m support.recording.manifest`
prints for it — see [`tests/CLAUDE.md`](tests/CLAUDE.md)'s "Detects
call-sequence drift, not semantic drift" for why `make test` alone doesn't
catch this.

CI (`.github/workflows/ci.yml`) runs `make test` split in two on every push
and pull request: `make lint` once, and `make test-cov` on every
supported OS and Python version in parallel; a final job combines their
coverage data and enforces the 95% gate on the combined number, so
platform- and version-specific branches count where they run. A local
`make test` enforces the same gate on one platform's run alone, which is
never higher than the combined one. `docs.yml` builds the Sphinx API docs
(`make docs`) on every pull request and push to `main`, and on push to
`main` deploys them to GitHub Pages. Before pushing a change
that touches a workflow file itself, run `make github-act-simulation` (needs
`act` and Docker) to exercise it locally.

## Release / publish channel

Public PyPI, via each package's own trusted-publishing environment in
`.github/workflows/release.yml` (`pypi-synology-apm-repo-sdk`,
`-cli`, `-browser`) — triggered by pushing a `v*` tag. A PyPI project must
have trusted publishing configured for that environment name once, by hand,
before its first `publish-*` job can succeed (same one-time-setup shape as
GitHub Pages needs — enabled once under Settings > Pages with "Source:
GitHub Actions" — or any other OIDC-trusted deploy target).
