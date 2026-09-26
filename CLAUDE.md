# CLAUDE.md — apm-repo-sdk-python Development Guide

> Read this file in full at the start of every new Claude Code session.

---

## Project Background

An offline, read-only reader for Synology ActiveProtect's dedup backup
repository format (APV/Object-Storage), delivered as three distributions
in one uv workspace — `synology-apm-repo-sdk`, `-cli`, `-browser`. See
[`ARCHITECTURE.md`](ARCHITECTURE.md)'s "What this project is" for
the full picture (audience, scope, per-distribution role); this file holds
only cross-cutting rules and pointers, not a second copy of it.

---

## Key Documents

See [`README.md`](README.md)'s Documentation Index for `ARCHITECTURE.md`,
`FORMAT-SPEC.md`, `CONTRIBUTING.md`, and `tests/CLAUDE.md` — not repeated
here. The rest are dev/internal-only and don't appear there:

| Document | Description | Priority |
|---|---|---|
| [`packages/synology-apm-repo-sdk/src/synology_apm_repo/sdk/README.md`](packages/synology-apm-repo-sdk/src/synology_apm_repo/sdk/README.md) | SDK-specific conventions and "adding a new backend / provider" recipes not already covered by `ARCHITECTURE.md`. | required when touching the SDK |
| [`packages/synology-apm-repo-cli/src/synology_apm_repo/cli/README.md`](packages/synology-apm-repo-cli/src/synology_apm_repo/cli/README.md) | CLI-specific command conventions and "adding a new command" recipe. | required when touching the CLI |
| [`packages/synology-apm-repo-browser/src/synology_apm_repo/browser/README.md`](packages/synology-apm-repo-browser/src/synology_apm_repo/browser/README.md) | TUI-specific screen/worker conventions and "adding a new screen" recipe. | required when touching the TUI |
| [`scripts/check_version_consistency.py`](scripts/check_version_consistency.py) | Checks the three packages' `project.version` fields and their cross-package dependency pins stay in lockstep. Run by `make test`, not invoked directly in normal workflow. | consult when a version bump or dependency pin edit isn't caught by `make test` |
| [`scripts/check_sdk_import_boundary.py`](scripts/check_sdk_import_boundary.py) | Verifies the CLI/browser packages import only the SDK's documented public surface. Run by `make test`, not invoked directly in normal workflow. | consult when a new SDK-facing import isn't caught by `make test` |
| [`.github/CLAUDE.md`](.github/CLAUDE.md) | GitHub Actions conventions: action-pin verification, what `make github-act-simulation` covers (build/test jobs, not deploy/publish), the PyPI trusted-publishing setup `release.yml` needs. | required when touching `.github/workflows/` |
| `docs/` | Sphinx build of `synology-apm-repo-sdk`'s API reference from its own Google-style docstrings (`make -C docs html`). See the SDK README's "Docstring Conventions" before adding an SDK module or docstring. | required when adding an SDK module |

New details belong in the closest document, not here — implementation
rationale goes in source docstrings/comments, layer conventions in
`ARCHITECTURE.md` or the relevant package README, investigation history
in commit messages. Each of the three package READMEs states up front that
it covers only what `ARCHITECTURE.md` doesn't already say — if you're
adding a convention that applies to more than one package, it belongs in
`ARCHITECTURE.md`, not repeated in each README.

---

## The One Rule That Matters Most

**Upper layers only touch the narrow contract of the layer directly below
them, and dependencies only point downward.** See `ARCHITECTURE.md` for the
full layer breakdown. New modules are placed and reasoned about using
`ARCHITECTURE.md`'s layer names (Codec/Storage/Dedup/Catalog/Content/Unit/
Repository) — don't re-embed layering rationale in source comments; a
comment at a specific boundary crossing can still explain *why that one
crossing exists*.

---

## Cross-Cutting Design Principles

### Language

Code, docstrings, comments, CLI help strings, and user-visible output strings
are **English**, no exceptions. Chat/commit-message discussion may mix
Chinese and English — that's fine for working conversation, not for anything
that ships in source.

### Async-native, everywhere

The whole SDK/CLI/TUI is `async def` end to end. Use `asyncio.to_thread()`
only to get a *single* blocking call off the event loop — see
`ARCHITECTURE.md`'s Async-native-by-design section for that boundary.
Concurrency added for throughput rather than responsiveness needs measuring
against a real sample first.

### Pythonic conventions

Every model/data class is `@dataclass(frozen=True)` — `Connection`,
`Workload`, `Version`, `RepoLayout`, `Node`, `RestorableUnit`, `Extent`,
and so on; a plain `@dataclass` without `frozen=True` is usually one of
these instead. Computed/derived attributes are exposed via `@property`.

Import the SDK's public surface from the top-level package
(`from synology_apm_repo.sdk import Session, ...`), never a submodule
path — except the CLI and TUI packages themselves, which import
exclusively via submodule paths (e.g. `from synology_apm_repo.sdk.api
import Repository`) as their own established convention, not a pattern
to extend elsewhere. `__all__` marks a genuine aggregation point — a
module that gathers names imported from elsewhere into one surface
(`sdk/__init__.py`, `sdk/api/__init__.py`, `sdk/storage/__init__.py`,
`sdk/profiles/__init__.py` today, also the one case mypy's
`no_implicit_reexport` requires it for); a module's own locally-defined
names are already public without one, and a lone pass-through name in a
module that doesn't otherwise aggregate anything uses the narrower `from
x import Y as Y` idiom instead (`api/repository.py`'s
`Finding`/`VerifyLevel`).

Don't add a wrapper whose only job is to route through a convention with
no actual transformation happening.

`raw.get(key) or default` for nested-JSON fields (e.g.
`catalog/workload.py`'s `workload_spec`, `units/content/saas_calendar.py`/
`saas_contact.py`'s `client_metadata`): a key can be present with a JSON
`null` or empty string, a case `.get(key, default)`'s default alone
doesn't cover.

### Docstring/comment discipline

A docstring or comment states a contract — what a reader needs before
touching the code — not a walkthrough of how it gets there, its history,
a rejected alternative, or a race/lifecycle explanation the code's own
structure already shows. Keep it short: one concern per docstring, no
pasted examples, no inline verification citations (state the one number
that matters, not how it was produced). When the same fact matters at
several call sites, state it once at each, trimmed to what that site
needs — never a shared essay one has to chase through another docstring.

SDK docstrings are Google-style (Napoleon), document the public surface
only, and must pass `make -C docs html O=-n`'s nitpick build; see the SDK
README's "Docstring Conventions" for the full rules.

### Presentation: users see backups, not a dedup repository

The default CLI/TUI view shows only user-facing backup concepts; internal
identifiers surface in `--verbose`/TUI's `d` verbose mode. See
`ARCHITECTURE.md`'s Presentation section before adding any new user-facing
display.

### Measure, don't assume — and disclose honestly

When a claim is checkable against real data, check it. When something wasn't
fully verified or a fix only covers part of the problem, say so explicitly —
in commit messages and in anything reported back about a task's completion.

---

## Testing Standards

See [`tests/CLAUDE.md`](tests/CLAUDE.md).

---

## Post-change Checklist (required before every commit)

- **Adding something new**: check the relevant package README's "Adding a
  new..." recipe first — each covers its own test/doc sync steps.
- **Any change that resolves an investigation, fixes a bug, or makes a design
  decision worth remembering**: write it into the commit message — root
  cause, real numbers, what was verified. If it changes the *stable* shape of
  the system, also update `ARCHITECTURE.md` or the relevant source
  docstring.
- **Any change**: `make test` — see `CONTRIBUTING.md`'s "Before every
  commit" for exactly what that runs and when a `tests/integration/`
  fixture also needs re-recording by hand.
- **Editing `ARCHITECTURE.md` or any other guideline doc**: verify each
  concrete claim you touch (a described default behavior, a cited count, an
  inline "quoted" section heading) against the actual current code/heading —
  these docs are prose, not code, so nothing but a careful read catches
  drift the way ruff/mypy/pytest catch it in source.

---

See [`CONTRIBUTING.md`](CONTRIBUTING.md) — [`README.md`](README.md)'s
Documentation Index lists what it covers.

