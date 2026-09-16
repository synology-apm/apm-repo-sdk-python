# Testing conventions

## Two kinds of test, and how they're told apart

The split is about one thing only: **is the data real or synthetic** —
not about whether a test needs anything from outside this repository to run.
Nothing in this suite needs real external state (a live sample tree, a
live network, real key material) at test-run time; every real-data test
replays bytes recorded once from a real sample, committed alongside it.

- **`tests/unit/`** — pure synthetic: hand-built bytes/fakes, no real
  sample ever involved, not even historically. The Codec Layer (`format/`)
  is entirely testable this way (hand-built bytes, no I/O).
- **`tests/integration/`** — real, production-shaped data via a
  `test_*.py` file replaying a committed `tests/fixtures/*.json.gz`
  fixture: `RecordingStore` captured a real sample's `ObjectStore` calls
  once, `ReplayStore` answers every one of them from the committed JSON,
  with zero real I/O at replay time — see "`RecordingStore` / `ReplayStore`"
  below for the recording/re-recording workflow. This is genuinely an
  integration-style regression test (it verifies real-format decoding
  against real bytes); it just doesn't need a live sample tree to *run*.
  A file living in `tests/integration/` doesn't mean every test in it
  touches real data, though — a Pilot test driven entirely by a fake
  `ContentSource`/`Session` belongs in `tests/unit/` even if it uses the
  same `App.run_test()` machinery as its real-data siblings
  (`test_browser_pilot_connect_dialog.py` and `test_browser_export_screen.py`
  are the current examples). Check what a test actually opens before assuming
  its file's directory settles the question.

## The `sdk`/`cli`/`browser` split

Both `tests/unit/` and `tests/integration/` are further split by which
distribution a test exercises — `sdk/`, `cli/`, `browser/`, mirroring
`synology-apm-repo-sdk`/`-cli`/`-browser`. Classify a new test file by what
it imports, not just its name: a file importing only
`synology_apm_repo.sdk.*` is `sdk/`, one importing `synology_apm_repo.cli.*`
is `cli/`, one importing `synology_apm_repo.browser.*` is `browser/` — a
`cli`/`browser` test incidentally also importing `sdk` for fixture setup
still belongs in `cli`/`browser` (expected, both distributions depend on
the SDK; what matters is the primary subject under test).

Both `tests/unit/` and `tests/integration/` live under the flat `tests/`
root — there is no per-package `tests/` directory; `packages/*/tests` does
not exist.

A file's basename is unique only within its own `unit`/`integration`
split, not across them — `test_units_device.py` exists in both
`tests/unit/sdk/` and `tests/integration/sdk/`, one of several
basename-collision cases in this suite. This is safe to collect and type-check
because `pyproject.toml` opts both tools into resolving by full
directory path rather than bare basename: pytest's own
`--import-mode=importlib`, and mypy's `mypy_path` including `"tests"`
alongside the package `src` dirs (`explicit_package_bases`/
`namespace_packages` alone aren't sufficient — verified empirically).
A same-named file needs no disambiguating suffix (e.g. a `_replay`
suffix) to avoid this collision — the directory already says it
replays, and the mechanism above already resolves the collision; don't
add one "to be safe" for a new same-named file.

**No test module ever imports from another — `tests/` isn't a package.**
This is why the same small "write fake on-disk bytes" builder helpers
(`_encode_size_store`, `_write_composition`, `_write_repo_info`, ...) show
up byte-for-byte identical across dozens of files instead of living in one
shared module: each test file is self-contained on purpose, so a change to
one file's fixtures can never silently ripple into an unrelated one's. If
you're fixing one of these builder helpers' bit-packing and it looks wrong,
grep for the other copies before assuming you found the only one — the
duplication is real, just not a sign nobody normalized it. This is
different from this project's sanctioned sharing mechanism: `tests/conftest.py`'s
`wait_until`/`open_browser_pilot` (shared across every distribution and
both the unit/integration split), `tests/unit/conftest.py`'s `fake_keyring`
(unit-only), and `tests/integration/cli/conftest.py`'s `patch_profile_store`
(CLI-integration-only) are all fixtures a genuinely repeated multi-line
setup sequence belongs in, not copy-pasted a 63rd time — pick whichever
conftest already matches how widely the new sequence is actually shared.

## `RecordingStore` / `ReplayStore` — this project's answer to cassette testing

A recorded fixture never asserts on backed-up content's own meaning (a real
file's content, a real message/list-row/photo's semantic value) — only the
repository's own structural/addressing correctness (catalog/workload/version
resolution, dedup chunk/composition addressing, disk fragment
reconstruction), even where proving that requires reading/hashing real bytes
as a structural oracle. A code path that can't be tested this way without
asserting on content's meaning doesn't get a real-data replay test — remove
it (or narrow it, below) rather than inventing a mechanism to smuggle the
real value in some other way; it gets a synthetic test instead, with
content-decoding correctness proven there.

This policy is actively enforced, not just stated: during a
`--record-against` session, `record_target`'s guard (`ContentRecordingBlocked`,
wrapping `RestorableUnit.open()` and `BucketReader.read_chunk`/`read_chunks`)
raises loudly the moment a test reads real backed-up content without
declaring it needs to, rather than letting real bytes quietly reach the
recorded fixture. `record_target(name, allow_content=True)` is the opt-out
for a test that genuinely needs real content bytes as a structural oracle
(a hash/signature check that never asserts on the content's own meaning) —
see `test_api.py`/`test_api_resolve.py`/`test_dedup_dedup_file.py` for real
examples, each with its own comment justifying the read.

`storage/recording.py`'s `RecordingStore`/`ReplayStore` — see
`ARCHITECTURE.md`'s "Cross-cutting shared mechanisms" for what it is;
reach for it before inventing a new synthetic-fixture mechanism for
Codec-through-Unit-Layer logic. Fixtures live in
`tests/fixtures/`, committed
gzip-compressed as `*.json.gz` (`write_fixture_text`/`load_fixture_text`/
`ReplayStore.from_path` handle the compression transparently). `ReplayStore`
raises immediately on an unrecorded call, so a fixture that's gone stale
fails loudly, not silently — see "Detects call-sequence drift, not semantic
drift" below for when re-recording is (and isn't) actually needed.

**Recording a fixture**: the replay test *is* the recipe.
`tests/integration/conftest.py`'s `record_target(name)` fixture returns
`ReplayStore.from_path(...)` on a normal run and a real-backend-wrapping
`RecordingStore` (writing the result back to `tests/fixtures/name` once
the test passes) when pytest is invoked with `--record-against=
local:<path>` or `--record-against=profile:<profile-name>`. Every
`test_*.py` file uses it; recording (or re-recording) a fixture is just:

```
make record-fixture TARGET=local:<path-to-sample-dir> \
    TEST=tests/integration/<path>/test_<name>.py::<test_function>
```

scoped to the one real root and test that fixture needs — one invocation,
one backend. This keeps a
fixture's recipe permanently in sync with the test it backs: a test whose
assertions no longer hold against the real backend (moved, renamed,
restructured data) simply fails and writes nothing, the same as any other
test failure. Every fixture belongs to exactly one real, always-running
replay test, whose own pass/fail is both its regression check and its
recording recipe.

A test that navigates to a specific real target keeps that navigation
recordable by using a **canonical** ref (`cat:<id>/wl:<id>/ver:<uid>`,
optionally `/device:<id>/object:<id>` deeper) rather than a human
display-name path — canonical segments are internal catalog identifiers,
immune to catalog-metadata anonymization, so the same literal ref string
resolves the same real node whether replaying an anonymized fixture or
recording fresh. A human ref built from real display names only ever
matches the *anonymized* text once it's committed, so recording it fresh
fails immediately (the real display name isn't "Test-Workload-4a5e").
Human-ref parsing/walking/disambiguation itself belongs in a synthetic
unit test (fake connections/workloads, no real backend) rather than a
replay test — see `tests/unit/cli/test_cli_tree.py`/`test_cli_ls.py` and
`tests/unit/sdk/test_units_node_ref.py` for the pattern.

**A replay test never hardcodes an anonymized display-name string as its
expected value** — a connection/workload `display_name`, a device's
`host_name`-derived name, a SaaS site/team/group name, or any other
field `scripts/anonymize_catalog_metadata.py`'s `SensitiveField`/`_POOLS`
list scrubs. That's synthetic unit-test territory: a fake, controlled
name proves the same rendering/parsing logic without ever drifting
across a re-recording. A replay test may still assert that such a value
*doesn't leak* somewhere it shouldn't (an id-only view), or compare one
freshly-fetched real value against another from the *same* recorded
session rather than a hardcoded literal — never the placeholder itself.
**A negative check (`not in`, `!=`) against a
specific anonymized string is a silent-failure trap**: once that
placeholder drifts on a later re-recording, the check keeps passing
without testing anything (the stale string was never going to appear
either way), unlike a positive check, which at least fails loudly —
prefer a structural check (a row/line count, a shape assertion) that
fails loudly either way. This doesn't apply to a version's own
`display_name` (a backup-time *timestamp*, never anonymized — see
"Timezone-rendered assertions" below), a fixed (not per-value)
placeholder like the `gws_domain` category's constant
`gwsdemo.example.com`, or a field never in `SensitiveField` to begin
with (`subtitle`, `workload_type`, `sub_type`, real disk/SaaS *content*
names like a filename or a SharePoint list's own fixed category labels).

If a replay test can't prove real-data resolution without materializing an
object's content (e.g. a `LazyArtifact`, whose `.size` is `None` until
assembled), narrow it further still — assert on the node's own listed shape
(kind, leaf-ness) instead of constructing a content unit at all, or push the
content-assembly proof to a synthetic unit test with fake data. See
`test_units_saas_mail.py`'s own docstring for the same pattern applied to a
whole workload type: real dispatch/listing only, content-assembly proven
synthetically instead.

A fixture recording an AHLT-encrypted `target.db` (any `copy_meta_file/
<vm>/target.db` under an encrypted vault) needs enough of its own
recording for the anonymizer's `target.db`-scrubbing pass to resolve a
vault key from it alone: a walk of `iter_layouts()` (which recurses up
to 2 levels, so the fixture can be rooted either directly at
`@ActiveProtectVault` or one level higher, at the sample directory) plus
one recorded `KeyMaterial.resolve_vault_key()` probe of its own — even
if no test in the file exercises that call directly — so its recording
carries the `exists()`/`listdir()`/`db/vault_encryption_key` reads both
passes need. Missing either raises `LookupError` at anonymize time
(loud, not silent — it runs inside `pytest_sessionfinish`, so it
surfaces even though the fixture's own replay tests still pass fine
against the un-scrubbed bytes), rather than depending on some other
fixture from the same sample having resolved the key first in the same
`anonymize_fixtures()` batch.

`record_target()` shares one `RecordingStore` across every test in the
run that requests the same fixture name (keyed at module level in
`tests/integration/conftest.py`, not rebuilt per test), writing it once at session
end — only if every test that touched that name passed. A fixture whose
tests don't subset each other (each covers a different scenario) merges
automatically this way: point `TEST=` at every test sharing the fixture,
or the whole file, in one `pytest --record-against=...` invocation —
`ReplayStore`/`RecordingStore` are already order-insensitive, so it
doesn't matter what order the sharing tests run in.

Anonymized placeholder text for a given real value isn't guaranteed
identical across separate recording sessions: `scripts/
anonymize_catalog_metadata.py`'s placeholder minting probes forward from
a value's preferred hash slot when it collides with another value's
preferred slot *already claimed in that same run* — so the same real
category name can land on a different placeholder in a run that
anonymizes a different set of real values alongside it. After
re-recording a fixture, check what the fresh output actually renders
(e.g. via `ReplayStore.from_path(...)` against the just-written fixture)
rather than assuming a previously-hardcoded placeholder string still
matches.

**Real-value constants repeated verbatim across files, by design**: a
few real, credential-shaped constants recur identically across several
self-contained replay test files (`tests/` isn't a package — see
above — so a shared value can't be imported, only duplicated). Each is
a real generated artifact belonging to one sample, not customer data,
and safe to commit for that reason — not because it merely looks inert:

| Constant | Belongs to | Appears in |
|---|---|---|
| `_ENCRYPTED_KEY_STRING`/`_APV2_ENCRYPTED_KEY_STRING` | `apv-sample-2-encrypted`'s own generated vault key | every replay test opening that sample's vault with a real key |
| `_S3SAMPLE2_ENCRYPTED_KEY_STRING` | `s3-sample-2-encrypted`'s own generated vault key | dedup/fingerprint-layer replay tests exercising its encryption |

A new file needing one of these copies the literal value from any
existing occurrence (already established as safe) rather than
re-deriving it or reading it from a real sample tree at test time. Add
a row here for any new repeated real-value constant rather than leaving
individual files to cite whichever one used it first.

Each `test_*.py` file owns its own dedicated fixture(s), recorded
independently even when a sibling file happens to open the same real
root — keeping every file's own `make record-fixture` invocation
self-sufficient is worth some duplicated bytes across `tests/fixtures/`
when two files touch the same real data. Multiple tests *within one
file* can share one fixture two ways: if one test's own calls are
already a superset of every sibling sharing the fixture, that single
test is the documented recording recipe (its own docstring says which) —
a narrower sibling simply exercises a slice of it, which `ReplayStore`
handles natively. Otherwise (each test covers a genuinely different
scenario, none a superset of the others), record all of them together in
one invocation — `record_target()`'s own session-wide sharing (above)
merges their calls automatically.

Every fixture is anonymized automatically afterward
(`scripts/anonymize_catalog_metadata.py`, via `tests/conftest.py`'s
`pytest_sessionfinish`) as part of the same recording step. Pass
`--no-anonymize` to skip it, e.g. to inspect the real bytes a recording
captured before they're scrubbed.

**No real value is ever written into this repository — not into a fixture, not
into a test's own source, not into an environment variable a test reads
by name.** A recording scenario that needs to locate one specific real
workload/device/version does it via a stable, non-identifying catalog
identifier (`workload_id`/`version_id`/`stream_uuid`/`connection_config_id`
— none of these are ever touched by catalog-metadata anonymization, so the
same id resolves the same real object whether replaying the anonymized
fixture or recording fresh against the real backend) rather than a real
name/email/path.

A committed `.json.gz` diffs as binary by default; see `CONTRIBUTING.md`'s
"Sample data" section for the one-time `git config` step that makes
`git diff`/`git log -p` on a re-recorded fixture readable text again.
Before adding a `record_target` call to a new replay test, check the
resulting fixture's *decompressed* size once recorded — a real "no
shortcuts" traversal (e.g. an uncapped recursive tree search) can rack up
thousands of distinct reads and grow to many MB even when the target data
itself is small, since `RecordingStore` stores every real `(path, offset,
length)` call verbatim with no cross-entry deduplication; that's a sign
the code path itself is expensive independent of real-vs-replayed I/O,
not something recording (or compressing the committed `.gz`) fixes — a
well-compressed but call-bloated fixture is still the same smell. This is
a one-time check at recording time, not something a fixture's own
docstring needs to state for a reader afterward — any number written
there (exact or bucketed) goes stale the moment a later re-recording
changes it, with nothing forcing the docstring to catch up; a test's own
docstring describes what a fixture covers, not how big it happens to be.

**Detects call-sequence drift, not semantic drift.** `ReplayStore` only
fails when a call it never recorded shows up — if production code starts
interpreting the *same* recorded bytes differently (a parsing bug fix at
the same offset/length, say), the fixture keeps answering the old bytes and
the test keeps passing against a now-stale interpretation. Nothing catches
this automatically; re-record by hand whenever the code path's *meaning*
changed, not only when its call shape did.

**A new fixture always gets its own file**, even when an existing one
already covers the same real root via the same entry point (`Session.
open_remote()`, bare `DedupRepo.open()`, `ConnectDialog.
_build_local_store()`'s TUI path, ...). This keeps `make record-fixture`
pointed at one test as the whole recipe for every fixture, file by file —
the fixture format (a flat `{reads, sizes, exists, listdirs}` dict) makes
merging two files' fixtures into one a trivial union, but the result
needs its own cross-file recording exercise to stay current. Some
duplicated real bytes across `tests/fixtures/` when two files touch the
same real root is the trade-off for every file staying independently
recordable.

## Disk image fixtures

A second, unrelated fixture family lives under `tests/fixtures/` too, and
under `tests/unit/sdk/` despite being real bytes rather than hand-built ones
— "no real sample ever involved" above means no real *repository/sample*
data (nothing recorded via `RecordingStore`/anonymized via
`scripts/anonymize_catalog_metadata.py`), not "no real bytes of any kind":
`test_units_disk_fs.py`'s ext4/XFS/Btrfs/NTFS and
`test_units_disk_fs_apfs.py`'s APFS tests replay real, byte-for-byte disk
images (`tiny_*.raw.gz`/
`tiny_*.raw.tar.gz`), parsed directly by `dissect.*` — nothing to do with
`ObjectStore`/`RecordingStore`/`ReplayStore` above. Each of those two test
modules' own docstring is the source of truth for its fixtures' build
recipe and storage format (plain gzip vs. a sparse-tar `.raw.tar.gz`, by
size) — this file doesn't duplicate that here.

## Timezone-rendered assertions

`catalog/version.py`'s `_version_display_name()` deliberately renders a version's
epoch in the machine's local timezone (see `ARCHITECTURE.md`'s Presentation
section). `tests/conftest.py`'s session-scoped `_fixed_timezone` fixture
pins `TZ` to `Asia/Taipei` for the whole suite so an assertion against that
rendered string reproduces on any machine, CI included — a replay test that
hardcodes a rendered timestamp string never needs to account for the
timezone itself, but do check a new one against the actual fixed value
rather than whatever your own machine happens to render.

## Closing a provider built directly against a repository

A test that constructs a `UnitProvider` directly (`RawObjectProvider.create()`,
`DeviceProvider.create()`, `FsProvider(...)`, `saas_provider_for()`, ...)
rather than going through `Repository.provider()` (which tracks and closes
its own) owns that provider's `SqliteSource`/`aiosqlite` connection and must
close it — an unclosed one leaks a background thread, surfacing (sometimes
on a *later*, unrelated test, since it depends on GC timing) as a
`ResourceWarning: ... was deleted before being closed` that the default
`make test` run doesn't even print. Every provider that needs this
implements `ClosableUnitProvider` (see the SDK README's Design Conventions),
so write `async with await XProvider.create(...) as provider:` — never a
bare `provider = await XProvider.create(...)` — the same way every
`DedupRepo.open()` call already goes through `async with`. This also
covers a helper that builds one for its own caller to keep using (returns a
still-open instance rather than closing it itself, mirroring
`tests/unit/sdk/test_units_device.py`'s `_provider_and_fs_node`): the *caller* wraps its own
use of the returned provider in `async with`/`try`-`finally`, never the
helper.

## Async tests

`asyncio_mode = "auto"` (root `pyproject.toml`) means `async def test_...()`
just works — no `@pytest.mark.asyncio` needed. Any test-local fake that
implements `ObjectStore` (there are several) must have all four methods as
`async def`. `@runtime_checkable`'s `isinstance()` check only looks at method
*presence*, not `async`-ness, so a sync-def fake passes that check and then
fails at the call site instead: a sync method returns a plain value that gets
`await`ed (`TypeError`), while an async method that never gets awaited hands
back an inert coroutine object.

## Before every commit

```
make test        # see CONTRIBUTING.md's "Before every commit" for what this runs
```

`make test` alone re-runs every `tests/integration/` fixture too, but see
"Detects call-sequence drift, not semantic drift" above — re-recording by
hand (`TARGET=local:<path>`) is a recording-tool action, not something any
test run does automatically.

## Real-sample smoke tests

`tests/smoke/` holds three separate, non-pytest tools -- `sdk/`, `cli/`,
`browser/`, one per distribution -- driven against real, on-disk sample
repositories configured in `tests/smoke/smoke_samples.toml` (a separate
mechanism from `tests/integration/`'s `--record-against=local:<path>`),
run by hand via `make smoke-test`, never by `make test`/CI. See
`tests/smoke/README.md`.

See [`CONTRIBUTING.md`](../CONTRIBUTING.md) for commit message conventions.
