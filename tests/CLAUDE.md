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

A `real shape` comment states structure only — which fields exist, how
they nest, or a byte layout — never a literal value copied from a real
sample. A literal the comment's own example needs (a name, an email, a
domain) draws from this suite's already-established synthetic pool
(`Alice`, `example.com`, `gwsdemo.example.com`, ...).

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
`tests/unit/sdk/` and `tests/integration/sdk/`. This is safe because
`pyproject.toml` resolves both tools by full directory path rather than
bare basename: pytest's `--import-mode=importlib`, and mypy's `mypy_path`
including `"tests"` alongside the package `src` dirs. A same-named file
needs no disambiguating suffix (e.g. `_replay`) to avoid this collision.

**No test module ever imports from another — `tests/` isn't a package.**
Small builder helpers (`_encode_size_store`, `_write_composition`,
`_write_repo_info`, ...) are duplicated byte-for-byte across dozens of
files by design, so a fixture change in one file never ripples into
another — grep for other copies before assuming a bit-packing fix is
needed in only one. The sanctioned sharing mechanism is a `conftest.py`
fixture instead: `tests/conftest.py`'s `wait_until`/`open_browser_pilot`
(shared everywhere), `tests/unit/conftest.py`'s `fake_keyring`
(unit-only), `tests/integration/cli/conftest.py`'s `patch_profile_store`
(CLI-integration-only) — pick whichever already matches how widely a
genuinely repeated setup sequence is shared.

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

This policy is actively enforced: during a `--record-against` session,
`record_target`'s `ContentRecordingBlocked` guard (wrapping
`RestorableUnit.open()` and `BucketReader.read_chunk`/`read_chunks`) raises
the moment a test reads real content without declaring it needs to.
`record_target(name, allow_content=True)` is the opt-out for a test that
genuinely needs real content bytes as a structural oracle — see
`test_api.py` for an example.

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

Every fixture belongs to exactly one real, always-running replay test,
whose own pass/fail is both its regression check and its recording recipe.

A test that navigates to a specific real target uses a **canonical** ref
(`cat:<id>/wl:<id>/ver:<uid>`, optionally `/device:<id>/object:<id>`
deeper), never a human display-name path — canonical segments are immune
to catalog-metadata anonymization, so the same ref string resolves the
same real node whether replaying an anonymized fixture or recording fresh
(a human display-name path only matches post-anonymization text, so
recording it fresh fails immediately). Human-ref parsing/walking belongs
in a synthetic unit test instead — see `tests/unit/cli/test_cli_tree.py`/
`test_cli_ls.py`/`tests/unit/sdk/test_units_node_ref.py`.

**A replay test never hardcodes an anonymized display-name string as its
expected value** — a connection/workload `display_name`, a device's
`host_name`-derived name, a SaaS site/team/group name, or any other field
`scripts/anonymize_catalog_metadata.py`'s `SensitiveField`/`_POOLS` list
scrubs. Test that logic with a synthetic unit test's fake name instead. A
replay test may assert a value *doesn't leak* somewhere it shouldn't, or
compare one freshly-fetched real value against another from the *same*
session — never against a hardcoded placeholder. A negative check
(`not in`, `!=`) against a specific anonymized string is a silent-failure
trap once that placeholder drifts on re-recording — prefer a structural
check (row/line count, shape assertion). Exceptions: a version's own
`display_name` (a timestamp, never anonymized — see "Timezone-rendered
assertions" below), the fixed `gws_domain` constant `gwsdemo.example.com`,
and any field never in `SensitiveField` (`subtitle`, `workload_type`,
`sub_type`, real disk/SaaS content names).

If a replay test can't prove real-data resolution without materializing an
object's content (e.g. a `LazyArtifact`, whose `.size` is `None` until
assembled), narrow it further still — assert on the node's own listed shape
(kind, leaf-ness) instead of constructing a content unit at all, or push the
content-assembly proof to a synthetic unit test with fake data.
`test_units_saas_mail.py` applies the same pattern at the whole-workload-type
level: real dispatch/listing only, content-assembly proven synthetically
instead.

A fixture recording an AHLT-encrypted `target.db` (`copy_meta_file/<vm>/
target.db` under an encrypted vault) must also record one
`KeyMaterial.resolve_vault_key()` probe — even if no test in the file
calls it directly — so the anonymizer's `target.db`-scrubbing pass can
resolve a vault key from this fixture alone (`iter_layouts()` recurses up
to 2 levels, so the fixture may be rooted at `@ActiveProtectVault` or one
level higher). A missing probe raises `LookupError` at anonymize time
(inside `pytest_sessionfinish`), not at test time.

`record_target()` shares one `RecordingStore` across every test in the
run that requests the same fixture name (keyed at module level in
`tests/integration/conftest.py`, not rebuilt per test), writing it once at session
end — only if every test that touched that name passed. A fixture whose
tests don't subset each other (each covers a different scenario) merges
automatically this way: point `TEST=` at every test sharing the fixture,
or the whole file, in one `pytest --record-against=...` invocation —
`ReplayStore`/`RecordingStore` are already order-insensitive, so it
doesn't matter what order the sharing tests run in.

Anonymized placeholder text for a real value isn't guaranteed identical
across separate recording sessions — a hash-slot collision with another
value present in that run can shift where it lands. After re-recording a
fixture, check what the fresh output actually renders (e.g. via
`ReplayStore.from_path(...)`) rather than assuming a previously-hardcoded
placeholder still matches.

**Real-value constants repeated verbatim across files, by design**: a
few real, credential-shaped constants recur identically across several
replay test files (`tests/` isn't a package, so a shared value can't be
imported, only duplicated). Each is a real generated artifact belonging
to one sample, not customer data, and safe to commit for that reason.

| Constant | Belongs to | Appears in |
|---|---|---|
| `_ENCRYPTED_KEY_STRING`/`_APV2_ENCRYPTED_KEY_STRING` | `apv-sample-2-encrypted`'s own generated vault key | every replay test opening that sample's vault with a real key |
| `_S3SAMPLE2_ENCRYPTED_KEY_STRING` | `s3-sample-2-encrypted`'s own generated vault key | dedup/fingerprint-layer replay tests exercising its encryption |

A new file needing one of these copies the literal value from an
existing occurrence rather than re-deriving it. Add a row here for any
new repeated real-value constant.

Each `test_*.py` file owns its own dedicated fixture(s), recorded
independently even when a sibling file happens to open the same real
root — keeping every file's own `make record-fixture` invocation
self-sufficient is worth some duplicated bytes across `tests/fixtures/`
when two files touch the same real data. Multiple tests *within one
file* can share one fixture two ways: if one test's own calls are
already a superset of every sibling sharing the fixture, that single
test is the documented recording recipe, and a narrower sibling simply
exercises a slice of it, which `ReplayStore`
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
`git diff`/`git log -p` readable again.

Before adding a `record_target` call to a new replay test, check the
resulting fixture's *decompressed* size once recorded: `RecordingStore`
stores every real `(path, offset, length)` call verbatim with no
cross-entry deduplication, so an uncapped traversal can grow to many MB
even when the target data is small — that's a sign the code path itself
is expensive, not something re-recording or compression fixes. Don't
record this number in the fixture's own test docstring; it goes stale on
the next re-recording.

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
_build_local_store()`'s TUI path, ...) — some duplicated real bytes
across `tests/fixtures/` is the trade-off for every file staying
independently recordable.

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
`ObjectStore`/`RecordingStore`/`ReplayStore` above. The build recipe and
storage format for each (plain gzip vs. a sparse-tar `.raw.tar.gz`, by
size) lives in that module's docstring, not duplicated here.

## Timezone-rendered assertions

`catalog/version.py`'s `_version_display_name()` deliberately renders a version's
epoch in the machine's local timezone (see `ARCHITECTURE.md`'s Presentation
section). `tests/conftest.py`'s session-scoped `_fixed_timezone` fixture
pins that timezone to `Asia/Taipei` for the whole suite so an assertion against
that rendered string reproduces on any machine, CI included (via `TZ`/`tzset`
on POSIX; Windows has neither, so there the fixture pins the one rendering
call site directly) — a replay test that
hardcodes a rendered timestamp string never needs to account for the
timezone itself, but do check a new one against the actual fixed value
rather than whatever your own machine happens to render.

## Driving a `Pilot` test: wait for the state, never for a duration

A `Pilot` test waits for the state a step needs via `tests/conftest.py`'s
`wait_until` — never `pilot.pause(n)` between an action and a read, which
passes on an idle machine and flakes on a busy one (usually a keypress
reaching a widget that isn't ready for it).

Three preconditions are easy to assume and must be waited on instead:
focus (wait for `has_focus` before pressing a key — `UnitScreen` granting
its tree focus takes a turn or two), cursor placement (`Tree.move_cursor()`
needs `_ = tree._tree_lines` forced first, then wait for `tree.cursor_node
is node`), and a rebuilt tree (`d`/`r` rebuild the whole tree, so
re-acquire any `TreeNode` picked before them rather than reusing it).

Use `tests/conftest.py`'s `ui_timeout` fixture for a synchronous UI change
(`push_screen`, a widget attribute) and `sdk_timeout` for anything gated on
a real SDK/Store/provider dispatch, even a fast one — `wait_until` returns
the instant its condition is true, so neither budget costs anything on a
healthy run. Exception: a condition that's a transient window closing on
its own (e.g. a loading indicator) keeps its own fixed, commented timeout
instead — a wider ceiling there can poll *after* the window already closed
(see `test_browser_pilot_hex_filter_refresh.py`).

A fixed `pause` is legitimate only to assert something *never* happens, or
to sample state at intervals on purpose — say so in a comment either way.

## Closing a provider built directly against a repository

A test that constructs a `UnitProvider` directly (`RawObjectProvider.create()`,
`DeviceProvider.create()`, `FsProvider(...)`, `saas_provider_for()`, ...)
rather than going through `Repository.provider()` (which tracks and closes
its own) owns that provider's `SqliteSource`/`aiosqlite` connection and must
close it. Every provider that needs this implements `ClosableUnitProvider`
(see the SDK README's Design Conventions), so write `async with await
XProvider.create(...) as provider:` — never a bare `provider = await
XProvider.create(...)` — the same way every `DedupRepo.open()` call already
goes through `async with`. This also covers a helper that builds one for its
own caller to keep using (returns a still-open instance rather than closing
it itself, mirroring `tests/unit/sdk/test_units_device.py`'s
`_provider_and_fs_node`): the *caller* wraps its own use of the returned
provider in `async with`/`try`-`finally`, never the helper.

A `monkeypatch.setattr(obj, "close", ...)`/`"aclose"` replacement that
simulates a close failure (or otherwise never calls through to the real
implementation) stops that real cleanup from ever running too — close the
real resource directly in the test's own `finally` block, or have the
replacement still delegate to the original close, rather than letting the
patch itself be the reason nothing real ever gets released.

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
