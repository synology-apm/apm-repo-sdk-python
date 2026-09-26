# `synology_apm_repo.sdk` — design conventions

See [`ARCHITECTURE.md`](../../../../../ARCHITECTURE.md) (repository root)
for the full layering contract first. This document covers conventions
specific to writing SDK code day to day.

## Design Conventions

Layering, read-only/no-network, cancellation, and the `to_thread()`
responsiveness-not-throughput rule are all in `ARCHITECTURE.md` — not
repeated here. Two conventions specific to writing SDK code day to day:

Everything is `async def` unless a method provably does none of it: a
new method that calls `ObjectStore`, `aiosqlite`, or anything downstream
of either is `async def`; one that's pure computation over
already-fetched data (`DedupFile.view()`, `Repository.key_status`) stays
sync. Anything that walks a potentially-huge sequence
(`DedupFile._extents()`, `CompositionRecord.entries()`,
`iter_repository_layouts()`, `Session.discover()`) is an
`AsyncIterator`/`AsyncGenerator`, never a materialized list — some of
these sequences are genuinely millions of items for a real 32 GiB VM
image (`_extents()` is private — see `chunk_walk.py`'s module docstring
for the one module allowed to call it directly).

A `UnitProvider` that owns a `SqliteSource`/`aiosqlite` connection
implements `ClosableUnitProvider` (`units/base.py`): `async def close()`
plus `__aenter__`/`__aexit__` delegating to it, the same pair
`SqliteSource`/`DedupRepo` already carry, so a caller writes `async with
await XProvider.create(...) as provider:` instead of a hand-rolled
`try`/`finally`. Use `async with` (not a bare assignment) at every call
site that constructs a provider directly, tests included.

## Docstring Conventions

SDK docstrings exist to survive a real Sphinx build without silently
rotting: `docs/` builds this package's public surface (only — CLI/TUI
docstrings aren't part of it) via `make -C docs html`, and a plain build
only warns on malformed RST — an unresolvable cross-reference target
renders as inert plain text instead of a link, with no warning at all.
Verify any of the conventions below with `make -C docs html O=-n` (the
nitpicky build, not a bare `sphinx-build`, which also skips the `apidoc`
step `docs/api/`'s gitignored RST stubs depend on) before trusting a new
or edited cross-reference.

Every docstring is Google-style, parsed by Sphinx's `napoleon` extension:
a one-line summary, optionally a short rationale/edge-case paragraph,
then `Args:`/`Returns:`/`Raises:` (or `Attributes:` for a
dataclass/`NamedTuple`) only where there's something non-trivial to say
about a parameter, return value, exception, or field — a trivial
one-liner stays a one-liner rather than manufacturing a block that just
restates the signature.

Reference another symbol by its bare name in ` ``double backticks`` `,
not a manual `:class:`/`:meth:`/`:func:`/`:attr:`/`:data:`/`:mod:` role:
`autodoc_typehints = "description"` (`docs/conf.py`) already
cross-references every annotated parameter/return type with zero markup,
so a role on top is a redundant second link, and unlike a backtick it
silently breaks the moment its target is renamed, moved, or made private
— exactly the kind of rot the nitpick build above exists to catch. The
one exception is a value object's own summary naming the one function
that constructs it, when the pairing is genuinely stable and one-to-one;
that role must be a full dotted path (`` :class:`~synology_apm_repo.sdk.storage.base.ObjectStore` ``) kept on one unwrapped line — a wrapped
qualified name embeds the newline into the target string, corrupting it.

Cite a `FORMAT-SPEC.md` subsection by its own heading term
(`FORMAT-SPEC.md: repo_info`), not its `§N.M` number, which shifts
silently whenever an earlier section is reordered — a whole chapter
(`§N`, no subsection) is the one exception, since chapters don't reorder.

Napoleon's parser reads a property/attribute docstring's text before its
*first* colon as a type-shorthand cross-reference target (the convention
behind `"""int: The width."""`), so an ordinary sentence like `"""Actual
bytes this chunk occupies: 4096 for ..."""` misparses into a broken
reference to a "class" literally named for that sentence — write the
first sentence colon-free, or push the colon-bearing detail to a second
sentence.

A public class keeps its immediate base public too, or built by
composition instead of inheritance: `autodoc_default_options` has no
`private-members` (so a `_`-prefixed name gets no page of its own
regardless of a docstring pointed at it), but `show-inheritance` prints
a class's base names verbatim, leaking a private base's name onto an
otherwise-public page.

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

1. Confirm which `TreeStrategy` (`units/saas/tree_strategy/`) matches the new
   workload's actual tree shape against real sample data — don't guess; there
   are already five distinct real shapes represented, and forcing a new one
   into the wrong shape produces subtly wrong navigation, not an error.
2. Write `assemble()` against real, sniffed sample bytes for that workload
   type before trusting any assumption about its service-DB schema — SaaS
   schemas drift across connector versions constantly (`storage/table.py`'s
   whole reason to exist). Put the actual byte-producing logic (building an
   EML/ICS/CSV, rendering HTML, ...) in a new `units/content/saas_<name>.py`
   module, not inline in `units/saas/<name>.py` — see `ARCHITECTURE.md`'s
   "Content Layer" section for that split's contract; `units/saas/<name>.py`
   keeps only the I/O-bound tree-navigation wiring that calls into it.
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
`storage/table.py`'s schema tolerance exists to absorb — see
`ARCHITECTURE.md`'s Storage Layer section for why schema drift across
connector versions is a given here, not an edge case.
