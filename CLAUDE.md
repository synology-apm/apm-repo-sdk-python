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
| `git log` | The investigation/decision history — "why does this code look like this." Root causes and verification steps live in commit messages, not a separate doc — see `CONTRIBUTING.md`'s commit convention. | consult when relevant |
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

- **Every model/data class is `@dataclass(frozen=True)`** — `Connection`,
  `Workload`, `Version`, `RepoLayout`, `Node`, `RestorableUnit`, `Extent`,
  and so on. If you're writing a plain `@dataclass` without `frozen=True`,
  check whether it should be one of these instead.
- **Computed/derived attributes are exposed via `@property`.**
- **Import the SDK's public surface from the top-level package**
  (`from synology_apm_repo.sdk import Session, ...`), not a submodule path.
  **Known exception**: the CLI and TUI packages themselves import
  exclusively via submodule paths everywhere (e.g.
  `from synology_apm_repo.sdk.api import Repository`) — that's their own
  convention, not the pattern to follow for anything new.
- **`__all__` marks a genuine aggregation point** — a module that gathers
  names imported from elsewhere into one surface (`sdk/__init__.py`,
  `sdk/api/__init__.py`, `sdk/storage/__init__.py`, `sdk/profiles/__init__.py`
  today), which is also the one case mypy's `no_implicit_reexport` actually
  requires it for. A module's own locally-defined names are already its
  public surface without one; a lone imported name meant to pass through
  a module that doesn't otherwise aggregate anything uses the narrower
  `from x import Y as Y` redundant-alias idiom instead (`api/repository.py`'s
  `Finding`/`VerifyLevel`), not a module-wide `__all__` of one entry.
- **Don't add a wrapper whose only job is to route through a convention with
  no actual transformation happening.**
- **`raw.get(key) or default`** for nested-JSON fields — e.g.
  `catalog/workload.py`'s `workload_spec` nested fields, and
  `units/content/saas_calendar.py`/`saas_contact.py`'s `client_metadata` —
  a key can be *present* with a JSON `null` or empty string, a case
  `.get(key, default)`'s default only covers when the key is missing
  outright. Follow the same pattern for new nested-JSON field access in
  this class of data.

### Docstring/comment discipline

A docstring is a contract, not a walkthrough: state what goes in, what comes
out, and the invariant(s) a caller must not violate. How the function gets
there step by step is already stated by the code a few lines below —
repeating it in the docstring risks the two copies drifting apart. One
concern per docstring: a function with several unrelated contracts gets one
short paragraph per contract at most, and reasoning that's really about one
specific branch belongs as a comment at that branch, not stacked into the
header above the signature.

State a docstring's contract in prose; a pasted code example only gives a
second copy of the mechanism a chance to drift from the real implementation.
Skip inline verification citations ("confirmed against sample X," "measured
Z% faster") — that belongs in the commit message that established it; a
constant whose magnitude needs justifying gets the one number that matters,
not the investigation that produced it. A module docstring orients rather than
catalogues: state what's in the file and the one or two facts worth knowing
before touching it — not a restatement of what its classes' and functions'
docstrings, one level down, already say.

If a docstring keeps growing, that's a signal to restructure — split it,
push detail into an inline comment at the line it explains, cut a citation
— not to compress the same content harder.

Describe what the code does now, not its history — how it used to work or
how a bug was found and fixed belongs in the commit message for the change.
When the same rationale applies at more than one call site, state it
directly at each one, trimmed to the single fact that site needs — a reader
looking at any one docstring/comment should get the reason without having
to chase it through another docstring or comment elsewhere.

SDK docstrings — the only ones built into API docs — are Google-style
(Napoleon), document the public surface only, and must pass a nitpick build
(`make -C docs html O=-n`) before trusting a new cross-reference; see the
SDK README's "Docstring Conventions" for the full rules.

### Presentation: users see backups, not a dedup repository

The default CLI/TUI view shows only user-facing backup concepts; internal
identifiers surface in `--verbose`/TUI's `d` verbose mode. See
`ARCHITECTURE.md`'s Presentation section before adding any new user-facing
display.

### Measure, don't assume — and disclose honestly

When a claim is checkable against real data, check it. When something wasn't
fully verified or a fix only covers part of the problem, say so explicitly —
in commit messages and in anything reported back about a task's completion.
See `CONTRIBUTING.md`'s Commit Convention for how this applies specifically
to a commit message's body.

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

*For detailed change history, see `git log`.*

