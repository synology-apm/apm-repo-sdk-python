# `synology_apm_repo.sdk` — design conventions

See [`ARCHITECTURE.md`](../../../../../ARCHITECTURE.md) (repository root)
for the full layering contract first. This document covers conventions
specific to writing SDK code day to day.

## Design Conventions

Layering, read-only/no-network, cancellation, and the `to_thread()`
responsiveness-not-throughput rule are all in `ARCHITECTURE.md` — not
repeated here. Two conventions specific to writing SDK code day to day:

- **Everything is `async def`.** A new method that does any I/O (calls
  `ObjectStore`, `aiosqlite`, or anything downstream of either) is `async def`.
  A method that provably does none (pure computation over already-fetched
  data — e.g. `DedupFile.view()`, `Repository.key_status`) stays sync —
  reserve `async def` for methods that actually await something.
- **Async generators, not "return a list."** Anything that walks a
  potentially-huge sequence (`DedupFile._extents()`, `CompositionRecord.entries()`,
  `iter_repository_layouts()`, `Session.discover()`) is an `AsyncIterator`/
  `AsyncGenerator`, not a materialized list — some of these sequences are
  genuinely millions of items for a real 32 GiB VM image. (`_extents()` is
  private — see `chunk_walk.py`'s module docstring for the one module allowed
  to call it directly and why.)
- **A `UnitProvider` that owns a `SqliteSource`/`aiosqlite` connection
  implements `ClosableUnitProvider`** (`units/base.py`): `async def close()`
  plus `__aenter__`/`__aexit__` delegating to it, the same pair
  `SqliteSource`/`DedupRepo` already carry. This is what lets a caller
  write `async with await XProvider.create(...) as provider:` instead of a
  hand-rolled `try`/`finally` — do this for any new provider that opens a
  connection, and use `async with` (not a bare assignment) at every call site
  that constructs one directly, tests included.

## Docstring Conventions

### Docstring format: Google-style (Napoleon)

Every docstring — module, class, function, method — is a **Google-style**
docstring, parsed by Sphinx's `napoleon` extension: a one-line
summary, optionally a short paragraph of the rationale/edge cases the
contract above requires, then `Args:`/`Returns:`/`Raises:` sections for a
function or method with something non-trivial to say about a parameter,
return value, or exception, or an `Attributes:` section for a
dataclass/`NamedTuple` whose fields need explaining beyond their name and
type annotation. A trivial one-liner (`"""Returns True when both host and
username are set."""`) stays a one-liner — don't manufacture an `Args:`/
`Returns:` block that would just restate the signature.

**Reference another symbol in prose by its bare name in
` ``double backticks`` `** — the same way you'd refer to it in a plain
comment, rather than a manual `:class:`/`:meth:`/`:func:`/`:attr:`/`:data:`/
`:mod:` role or a dotted path. `autodoc_typehints = "description"` (see
`docs/conf.py`) already turns every parameter and return type annotation in
a signature into a working cross-reference with zero markup — if a function
takes or returns some class, that class is already linked from the type
annotation alone, so a hand-written role on top of it is a second,
redundant link. A backticked name also keeps working for as long as the
symbol exists, while a role silently breaks the moment the symbol it names
is renamed, moved, or made private (Sphinx renders the broken one as inert
plain text with no warning outside a `-n` build — see below). The one
narrow exception is a value object's own one-line summary naming the exact
function that constructs it when the two are a genuinely stable, one-to-one
pair (mirroring that function's own `Returns:` already pointing back at the
class) — reserve a role for that pairing alone.

**Cite a `FORMAT-SPEC.md` subsection by its own stable name, never by its
`§N.M` number** — e.g. `FORMAT-SPEC.md: repo_info` (matching that
subsection's own heading term, or a short slug when the heading has no
single-word subject), not `FORMAT-SPEC.md §2.5`. Same reasoning as the
backticked-name rule above: a `§N.M` number shifts whenever an earlier
section is added, removed, or reordered, silently breaking every citation
after it with no warning anywhere. A whole chapter (`§N`, no subsection) is
the one exception still cited by number, since chapters don't reorder.

**Write a property or bare-attribute docstring's first sentence colon-free
up to its first period.** Napoleon's Google-style parser treats a
property/attribute docstring's text before its *first* colon as a
type-shorthand annotation (the documented convention behind `"""int: The
width."""`) and turns it into a cross-reference target — so an ordinary
English sentence like `"""Actual bytes this chunk occupies in the data
region: 4096 for ..."""` gets misread as an attempt to cross-reference a
"class" literally named "Actual bytes this chunk occupies in the data
region", producing a nitpick warning with no real role written anywhere to
fix. Every type is already spelled out by the signature/annotation itself
(`autodoc_typehints`), so this shorthand is unnecessary here — write the
first sentence colon-free, or move the colon-bearing detail to a second
sentence.

### API docs expose the public surface only

`docs/`'s Sphinx build documents `synology-apm-repo-sdk`'s public surface
only — `autodoc_default_options` has no `private-members`, so a `_`-prefixed
name gets no page of its own, regardless of a docstring or role pointed at
it. A **public** class keeps its immediate base public too (or built via
composition instead of inheritance): `show-inheritance` prints a class's
base names verbatim, so a private implementation-sharing base class's name
would otherwise leak onto that public page.

### Sphinx cross-reference builds: keep full paths on one line

`docs/` builds `packages/synology-apm-repo-sdk` (only — CLI/TUI docstrings
aren't part of this build) via `make -C docs html`. A plain build only warns
on malformed RST; an unresolvable `:class:`/`:func:`/`:meth:`/`:data:`/`:attr:`
target is *not* a warning there — Sphinx silently renders it as inert plain
text instead of a link. Run `make -C docs html O=-n` (the `-n`/nitpicky
flag, passed through `$(O)`) before trusting a new or edited one — not a
bare `sphinx-build` invocation, which skips the `apidoc` step `docs/api/`'s
RST stubs depend on (that directory is gitignored, populated only by this
target) and errors on unresolved toctree entries from a clean checkout. The
nitpicky build turns every unresolved target into a warning instead — this
is what catches a role slipping in anyway, or the colon-shorthand trap
above getting tripped by accident.

For the one narrow case above that does still use a role: it must be a full
path (`` :class:`~synology_apm_repo.sdk.storage.base.ObjectStore` ``, the
`~` only shortens the *displayed* text, not the resolution) — an unqualified
role only resolves inside the module/class that defines the target itself.
Keep its backtick-quoted target on one line — a wrapped long qualified name
embeds the newline and indentation into the target string, corrupting it;
let the line run long instead.

## Adding a New Storage Backend

1. Implement the four `ObjectStore` methods (`read`/`size`/`exists`/`listdir`),
   `async def`, in a new module under `storage/`.
2. Decide honestly whether the backend has genuine non-blocking I/O (network,
   like `S3Store`/`AzureStore` via `aiohttp`) or needs `asyncio.to_thread()`
   wrapping over a synchronous body (local-file-shaped, like `LocalFsStore`) —
   see `ARCHITECTURE.md`'s async section for which is which and why.
3. Add it to the shared parametrized `ObjectStore` contract tests
   (`tests/unit/sdk/test_storage_object_store_contract.py`) so it's held to the
   same behavior (EOF short-reads, `NotFoundError` mapping, pagination) as every
   other backend — deviations must be an explicit, documented, tested
   difference, not an accident.
4. If it needs a third-party dependency, add it as a plain dependency in
   `packages/synology-apm-repo-sdk/pyproject.toml`'s `[project.dependencies]`
   — there are no optional extras in this SDK, everything ships together.
   If that dependency is itself a heavy import (network SDKs like
   `aioboto3`/`azure-storage-blob` are the existing examples), still import
   it lazily inside the module (not at `storage/__init__.py` level, which
   imports every backend module unconditionally) so callers who only use
   `LocalFsStore` don't pay for it.

A capability that doesn't fit any single bucket/container/root — listing
what buckets/containers exist at all, say (`storage/s3.py`'s
`list_buckets()`, `storage/azure.py`'s `list_containers()`) — isn't an
`ObjectStore` method (every one of those is already scoped to one chosen
bucket/container) and doesn't go through a backend's own class. Add it as a
free function beside the class instead, following the same lazy-import
pattern; callers needing it (the TUI's connect dialog, for a "browse
buckets"/"browse containers" action) call the function directly rather than
reaching past it for the underlying client library.

## Adding a New SaaS Workload Provider

Most SaaS workload types (Mail/Drive/Contact/Calendar/Site) are a
`SaasWorkloadConfig` value plus an `assemble()` function on top of the shared
`SaasWorkloadProvider` base (`units/saas/provider.py`) — not a new provider
class. Before writing a new one:

1. Confirm which `TreeStrategy` (`units/saas/tree_strategy.py`) matches the new
   workload's actual tree shape against real sample data — don't guess; there
   are already four distinct real shapes represented, and forcing a new one
   into the wrong shape produces subtly wrong navigation, not an error.
2. Write `assemble()` against real, sniffed sample bytes for that workload
   type before trusting any assumption about its service-DB schema — SaaS
   schemas drift across connector versions constantly (`storage/table.py`'s
   whole reason to exist). Put the actual byte-producing logic (building an
   EML/ICS/CSV, rendering HTML, ...) in a new `units/content/saas_<name>.py`
   module, not inline in `units/saas/<name>.py` — that split is the Content
   Layer's whole reason to exist (see `ARCHITECTURE.md`'s "Content Layer"
   section); `units/saas/<name>.py` keeps only the I/O-bound tree-navigation
   wiring that calls into it.
3. Only build a fully independent provider (like `TeamsChatProvider`) if the
   service-DB *location* mechanism itself is genuinely different from "look up
   one fixed table name" — that's the actual dividing line, not workload type
   per se.
4. Wire it into `units/dispatch.py::saas_provider_for()`'s candidate list, with
   a fallback to `RawObjectProvider` preserved for when detection fails.

The Device (`units/device.py`) and FS (`units/fs.py`) provider families have
no equivalent "adding a new one" recipe — there is exactly one of each.
SaaS is the one family designed to grow.

## APM Version Compatibility

This SDK decodes an on-disk format, not a versioned API — compatibility is
governed by which sections of `FORMAT-SPEC.md` a given repository sample
actually matches, not by a version number the SDK negotiates. When a
schema/format difference shows up across samples, that's exactly what
`storage/table.py`'s schema tolerance and the trap list in
`ARCHITECTURE.md` exist to absorb.
