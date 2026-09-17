# Contributing

See [`CLAUDE.md`](CLAUDE.md) for the development guide and the "Install From Source"
section of [`README.md`](README.md) for environment setup.

## Commit Convention

```
feat:  new feature        feat: add zstd dictionary support to ObjectStore
fix:   bug fix            fix: correct CRC verification on multi-chunk reads
docs:  documentation      docs: clarify RestorableUnit's extent invariant
test:  tests              test: close coverage gap for dedup chunkmap dump
chore: configuration      chore: bump uv.lock via make bump-external-versions
```

Body: describe what was actually found and verified, not just what changed.
This project's culture (see `git log` for dozens of examples) is to cite
real numbers — exact test counts, coverage deltas, real sample names,
before/after benchmark measurements — rather than describe a change in the
abstract. If something was deliberately left undone, say so and say why; a
reader should be able to tell "not done because it doesn't matter" from "not
done because it's genuinely unresolved" from the commit message alone — the
commit message *is* this project's investigation/decision record.

**Only claim something passed, was measured, or was verified when you
actually ran it in this exact session.** A number written into a commit
message that wasn't actually produced by a real run is worse than not having
the number at all — it looks like evidence and isn't. If a full verification
pass (ruff/mypy/pytest) wasn't possible for some reason, say that plainly
rather than omit it.

## Committing a subset while other changes are staged

If other files are already `git add`-ed — e.g. leftover from parallel
agent/session work — and you only want to commit a specific subset, commit
just that subset with `git commit -- <paths>`. A bare `git commit` commits
the entire index regardless of what you just staged. If a commit picks up
more than intended, `git reset --soft HEAD~1` restores the prior index state
so you can redo it correctly.

## Sample data

See `tests/CLAUDE.md`'s "RecordingStore / ReplayStore" section for what a
recorded fixture proves (repository structure, never backed-up content's
own meaning) and how to record one.

What a recording captures from the real sample's catalog layer (`db/
connection_config`, `workload_config`, `copy_target_version`, `file_map`,
`file_meta`, and similar SQLite files) is anonymized automatically at
recording time — deterministically, the same real value producing the
same placeholder across runs unless it collides with a different real
value's preferred hash slot in that particular run (in which case the
other real values present that run can shift where it lands — see
`tests/CLAUDE.md`'s "RecordingStore / ReplayStore" section) — see
`scripts/anonymize_catalog_metadata.py`'s own docstring for exactly which
fields and what each placeholder looks like.

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

See the `Makefile` for exactly what that runs (format check, lint, mypy,
pytest with the 95% coverage gate enforced, version-consistency check,
SDK import-boundary check) — the whole suite runs every time, no real
external state needed. The 95% gate applies uniformly across the SDK,
CLI, and TUI — there is no package-level
carve-out. The only lines excluded from it are individual, justified
`# pragma: no cover` markers, always scoped to the individual line, not a
whole file or package, in one of three categories: real process/terminal
I/O (e.g. `cli/main.py`'s and `browser/app.py`'s own `main()`, which call
`sys.exit`/open a real terminal and so can't run under `CliRunner`/
`App.run_test()` the way everything else does); a defensive/unreachable
branch (a `NotImplementedError` stub every real subclass overrides, an
exhaustiveness fallback after an already-exhaustive enum/`match`); or a
real coverage.py false negative, confirmed directly via `sys.settrace`
(bypassing coverage.py entirely) before reaching for the pragma — that
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
against, re-record the affected fixture by hand
(`make record-fixture TARGET=local:<path> TEST=tests/integration/<path>/test_<name>.py::<test_function>`)
— see [`tests/CLAUDE.md`](tests/CLAUDE.md)'s "RecordingStore / ReplayStore"
section for when and why `make test` alone doesn't catch this.

CI (`.github/workflows/ci.yml`) runs the same `make test` on every push and
pull request, plus `make test-unit` across the older supported Python
versions. `docs.yml` builds the Sphinx API docs the same way on every push/PR
and, on push to `main`, deploys them to GitHub Pages. Before pushing a change
that touches a workflow file itself, run `make github-act-simulation` (needs
`act` and Docker) to exercise it locally.

## Release / publish channel

Public PyPI, via each package's own trusted-publishing environment in
`.github/workflows/release.yml` (`pypi-synology-apm-repo-sdk`,
`-cli`, `-browser`) — triggered by pushing a `v*` tag. A PyPI project must
have trusted publishing configured for that environment name once, by hand,
before its first `publish-*` job can succeed (same one-time-setup shape as
GitHub Pages needs — see `docs.yml`'s own header comment — or any other
OIDC-trusted deploy target).
