# Testing conventions

## Two kinds of test, and how they're told apart

The split is about one thing only: **is the data real or synthetic**. Every
test runs with no external state (no live sample tree, network or real key
material); a real-data test replays bytes recorded once from an actual
backup repository and committed alongside it.

- **`tests/unit/`** — synthetic: hand-built bytes and fakes, no recorded
  data involved. The Codec Layer (`format/`) is entirely testable this way.
- **`tests/integration/`** — real, production-shaped data: a `test_*.py`
  file replays a committed `tests/fixtures/*.json.gz` fixture through
  `ReplayStore` (see "`RecordingStore` / `ReplayStore`" below). Classify by
  what a test actually opens: a Pilot test driven entirely by a fake
  `ContentSource`/`Session` belongs in `tests/unit/`
  (`tests/unit/browser/test_browser_screens_connect_dialog.py`).

A `real shape` comment states structure only — which fields exist, how
they nest, or a byte layout. A literal its example needs (a name, an email,
a domain) comes from this suite's synthetic pool (`Alice`, `example.com`,
`gwsdemo.example.com`, ...).

## Layout and naming

Both splits divide further by the distribution a test exercises — `sdk/`,
`cli/`, `browser/` — classified by its primary subject: a `cli`/`browser`
test that also imports the SDK for setup stays in `cli`/`browser`. All tests
live under the root `tests/`, not per package.

Code outside the three distributions is tested under `tests/unit/scripts/`
(`scripts/*.py`) and `tests/unit/examples/` (`examples/**/*.py`, named by
the module's stem alone: `examples/hypervisor-restore/stream_vmdk.py` →
`test_stream_vmdk_<behaviour>.py`). Each test file loads its module by path
with `support.modules.load_module` through a fixture — for an example,
`tests/unit/examples/conftest.py`'s module-scoped `ex`, picked by the test
file's name — and changes the module's attributes only through
`monkeypatch`. An example's third-party packages this repository doesn't
depend on (`ntnx_*`, `iscsi`, `pyVmomi`), and a hypervisor's REST API, are
reached only through seams in the example itself (`NutanixSdk`,
`_load_iscsi`, `VsphereSdk`, `PveApi`), which the tests replace with fakes;
what those seams do against the real service is checked by a run against
it. Coverage measures only `synology_apm_repo`, so neither `scripts/` nor
`examples/` counts toward its gate.

A test file is named after the source module or package it tests: its
path relative to the distribution's package, each part's leading
underscores dropped, `__init__.py` standing for its package, joined by `_`,
and prefixed by the distribution for `cli`/`browser`
(`sdk/dedup/export_workers.py` → `test_dedup_export_workers.py`,
`sdk/units/content/disk_fs/_apfs.py` → `test_units_content_disk_fs_apfs.py`,
`cli/commands/export.py` → `test_cli_commands_export.py`,
`browser/core/browse/repo_labels.py` → `test_browser_core_browse_repo_labels.py`).
A file covering one behaviour of a larger module appends it:
`test_<module path>_<behaviour>.py` (`test_browser_screens_unit_screen_goto.py`,
`test_cli_commands_export_cancel.py`). A test of a cross-cutting invariant
names the narrowest package it covers (`test_storage_readonly_invariant.py`,
`test_api_public_surface.py`), and a Pilot test driving the whole app
names `app` (`test_browser_app_copy_and_goto_ref.py`). The same rule names
`tests/unit/scripts/test_<script>[_<behaviour>].py`,
`tests/unit/examples/test_<example>_<behaviour>.py` and
`tests/unit/support/test_<tests/support module>[_<behaviour>].py`, which
tests this suite's own tooling; a check over the suite's own layout there
is `test_test_layout[_<behaviour>].py`. `tests/unit/support/test_test_layout.py`
checks every `test_*.py` under `tests/unit/` and `tests/integration/`
against the real source tree. A basename need only be unique within its
split (`test_units_device.py` exists in both `tests/unit/sdk/` and
`tests/integration/sdk/`): `pyproject.toml` has pytest and mypy resolve
test files by path.

**Shared test code lives in importable modules; test files import only
those.** `pyproject.toml`'s `pythonpath = ["tests"]` makes each directory
under `tests/` a namespace package, so a module is imported by its path
from there (`support.repo_builders`, `unit.sdk.api_fakes`,
`integration.browser.pilot_drivers`); pytest collects only `test_*.py`, and
`test_test_layout.py` checks that no module imports one. Place shared code
by how widely it is used:

- `tests/support/` — used across splits or distributions, all synthetic:
  - `format_builders.py`: the write-side encoder of each on-disk structure
    (bucket header, SizeStore and its padded region, ChunkCrcStore,
    Redundancy blob, chunk address, ChunkMapRecord with its
    `mapping_record`/`zero_record` shorthands, RecordHead, composition
    header, `.inf` header, `repo_info`, `repo_transaction`, an ObjectDB),
    pure bytes-in/bytes-out with every field a parameter and nothing
    imported from the SDK; `tests/unit/support/test_format_builders.py`
    round-trips each through the SDK's parser by one hand-checked example.
    A layout fix lands there once.
  - `repo_builders.py`: puts those structures on disk under `tmp_path`
    with real content (`write_bucket`, `write_composition`,
    `write_composition_entries`, `write_file_map`, the `db/*` tables — the
    `copy_target_*` and `workload_config` writers append, so one repository
    can hold several workloads — and SaaS version/snapshot dbs), and builds
    whole-file bytes (`filler_bucket_bytes`, `uncompressed_bucket_bytes`,
    `composition_record_bytes`). A writer that carries one test file's own
    scenario (a different schema, a deliberately broken field) stays a
    private helper in that file.
  - `model_factories.py`: `make_connection`, `make_workload`,
    `make_version` (a SaaS version is the same call with its `saas_*`
    fields) and `make_catalog`; each model field is a keyword argument with
    a synthetic default, identifiers taken as plain `int`/`str`. A test
    file keeps a wrapper only to pin its own scenario (its stream uuid, its
    target type), and that wrapper calls the factory.
  - `store_fakes.py`: fake `ObjectStore`s — `WrappingStore` (pass-through
    base; a test-local wrapper overrides only the call it instruments),
    `BlockingStore`, `CountingStore`, `LoopCheckingStore`, `CloseCountingStore`,
    `FailingStore`.
  - `content_fakes.py`: `BlockingContentSource`, whose export runs until
    cancelled, and `TreeProvider`, a fixed node tree over given contents.
  - `fakes.py`: `@faithful_to(Real)` and `@unchecked_fake(...)` (below).
  - `pilot.py`: Textual `Pilot` waits, `RUN_TEST_SIZE`,
    `count_progress_ticks` (see "Driving a `Pilot` test"), and the autouse
    `fast_browser_debounce` fixture both `browser/` directories' conftests
    import.
  - `disk_fs_sibling.py`: the `no_disk_fs_sibling` fixture
    `tests/unit/sdk/` and `tests/integration/cli/` import into their
    conftests.
  - `cli.py`: `invoke(args, *, input=None, exit_code=0)` runs the CLI
    in-process and fails the test, with the command's output, unless the exit
    code matches; `exit_code=None` when the test inspects the outcome itself.
  - `modules.py`: `load_module(name, path)`.
  - `recording/`: `fixture_store.py` (see "`RecordingStore` /
    `ReplayStore`"), and `sample_constants.py`, the recorded samples' own
    generated values.
- `<split>/<distribution>/*_fakes.py` / `*_drivers.py` — fakes, builders and
  multi-step drives that several test files of one component share:
  `unit/browser/` (`screen_host_fakes`, `unit_screen_fakes`,
  `browse_screen_fakes`, `connect_dialog_drivers`), `unit/sdk/` (`api_fakes`,
  `catalog_fakes`, `dedup_export_fakes`, `device_fakes`, `disk_fs_fakes`,
  `pool_fakes`, `saas_fakes`, `storage_fakes`, `tree_strategy_fakes`,
  `verify_reachable_fakes`, and the hypothesis strategies in
  `format_strategies`), `unit/cli/` (`export_fakes`, `listing_fakes`,
  `session_fakes`), `unit/examples/hypervisor_restore_fakes`,
  `integration/browser/pilot_drivers`, `integration/sdk/vault_key_drivers`.
  `tests/unit/support/test_test_layout_duplicate_helpers.py` fails when two
  test files define a structurally identical top-level function or class of
  five or more statements (names and docstrings aside); the copy belongs in
  one of these modules. A recorded or on-disk fixture's loader stays with
  the test file that owns the fixture.
- `conftest.py` — only setup that needs pytest's fixture machinery (another
  fixture, or teardown), in the narrowest directory that uses it:
  `tests/integration/conftest.py`'s `record_target`,
  `tests/integration/browser/conftest.py`'s `replay_local_store`,
  `tests/integration/cli/conftest.py`'s `patch_profile_store`,
  `tests/unit/conftest.py`'s `fake_keyring`, and
  `tests/unit/examples/conftest.py`'s `ex`. A fixture several directories
  need is defined once in a `tests/support/` module and imported by name
  (`from support.pilot import fast_browser_debounce as
  fast_browser_debounce`) into each directory's conftest: both `browser/`
  directories' autouse `fast_browser_debounce`, and `no_disk_fs_sibling` in
  `tests/unit/sdk/` and `tests/integration/cli/` (a module opts in with
  `pytest.mark.usefixtures`). `tests/conftest.py` holds only
  session-wide setup (the fixed timezone, Rich seeing no terminal and 80
  columns, the hypothesis profiles, applying the `pilot` marker).

A hand-written fake standing in for an SDK class (`Repository`, `Catalog`,
`Session`, `UnitProvider`, `ContentSource`, `DedupRepo`, `ObjectStore`, ...)
carries `@faithful_to(Real)` from `tests/support/fakes.py`, which fails
collection when a public method the fake (or a subclass) defines differs
from the real one's parameters or sync/async-ness; a fake of an object
implementing several protocols names them all (`@faithful_to(UnitProvider,
SupportsDirectRefLookup)`). Members a fake adds (counters, gates,
switches) are underscore-prefixed or plain attributes. A fake with no real
class to compare against — a third-party API (`boto3`, `azure`,
`smbclient`, `dissect`, `ntnx_*`) or an `examples/` seam loaded at test
time — carries `@unchecked_fake("<what it stands in for>")` instead.
`tests/unit/support/test_fakes_conventions.py` checks that every fake-shaped
class under `tests/` carries one of the two.

## Property tests

A Codec Layer module's hypothesis properties live in
`tests/unit/sdk/test_format_<module>_properties.py`, next to its example
tests in `test_format_<module>.py`, with shared strategies in
`unit/sdk/format_strategies.py`. Two properties per structure: an encoding
of generated fields (through `format_builders` where it has an encoder)
round-trips through its parser, and any bytes — arbitrary, or a valid
encoding truncated or with one byte flipped — either parse or raise the
parser's documented error (a `FormatError` for on-disk bytes). An exception
found that way is an SDK bug, fixed with an example-based regression test
beside the fix.

`tests/conftest.py` selects the settings profile from `HYPOTHESIS_PROFILE`:
`default` (50 examples, for local runs) or `ci` (200 examples, set by
`ci.yml`, which also caches `.hypothesis/` so a failure found once replays
first). Both drop the deadline; a test sets no `max_examples` of its own,
so the profile scales every property alike.

## `RecordingStore` / `ReplayStore` — this project's answer to cassette testing

`tests/support/recording/fixture_store.py`'s `RecordingStore` (built on the
SDK's `InstrumentedStore`) wraps a real `ObjectStore` and records every
call/result pair, a `NotFoundError` as a recorded miss; its `ReplayStore`
answers the same calls with zero real I/O. An unrecorded call raises
`UnrecordedCallError`, which is not an SDK error, and `record_target` fails
the test at teardown for any unrecorded call, even one the code under test
caught, so a stale fixture fails loudly. `ReplayStore` reads only the
current fixture format (`"format": 2`) and raises `FixtureFormatError`
for any other. Reach for it before inventing
a new fixture mechanism for Codec-through-Unit-Layer logic. Fixtures live
in `tests/fixtures/`, committed gzip-compressed as `*.json.gz`.

**What a replay test asserts.** A recorded fixture proves the repository's
structural/addressing correctness (catalog/workload/version resolution,
dedup chunk/composition addressing, disk fragment reconstruction), reading
or hashing real bytes as a structural oracle where needed — never
backed-up content's own meaning (a real file's content, a message's
subject, a photo). A code path that can't be proven without asserting on
content's meaning gets a synthetic test instead. `record_target` installs
a `ContentRecordingBlocked` guard on every `RestorableUnit.of()`'s content
and on `BucketReader.read_chunk`/`read_chunks`/`read_chunk_blocks`, on a
replay as on a recording, so a test that reads real content fails the same
way whether or not the fixture covers the read;
`record_target(name, allow_content=True)` is the opt-out for a
test that needs real content bytes as a structural oracle (see
`tests/integration/sdk/test_dedup_repository.py`). If a replay test can't
prove real-data resolution without materializing content (e.g. a
`LazyArtifact`, whose `.size` is `None` until assembled), assert on the
node's listed shape (kind, leaf-ness) and prove content assembly
synthetically. SaaS item content is never recorded at all: the
`tests/unit/sdk/test_units_saas_*.py` files cover it with synthetic data.

**Where a test's store comes from.** Every integration test gets its
store through `tests/integration/conftest.py`'s `record_target(name)`,
directly or through `replay_local_store`/`patch_profile_store`, which
returns `ReplayStore.from_path(tests/fixtures/<name>)`. The test *is* the
fixture's recipe: the calls it makes are the calls the fixture holds.
Each fixture belongs to one `test_*.py` file, and a sample is named by its
alias everywhere — the alias says what the sample holds (`vault-plain`,
`objstore-encrypted`, `pcps`), and a fixture name, a constant, a
docstring, a comment and a recorded path all use it
(`cli_doctor_vault_plain.json.gz`, `VAULT_ENCRYPTED_KEY_STRING`).
A change that needs a new or re-recorded fixture says so in its pull
request.

A committed `.json.gz` diffs as binary by default; `CONTRIBUTING.md`'s
"Sample data" section has the one-time `git config` step for readable
diffs.

**Detects call-sequence drift, not semantic drift.** `ReplayStore` fails
only on a call it never recorded. If production code starts interpreting
the *same* recorded bytes differently, the test keeps passing against the
stale interpretation, so a fixture needs re-recording whenever a code
path's *meaning* changes, not only its call shape.

### Anonymized fields and sample constants

**Everything this repository stores is synthetic or anonymized** — every
fixture, every test's source, every environment variable a test reads by
name. `tests/unit/support/test_recording_fixture_privacy.py` checks the
fixtures: it decodes every read and every chunk of each one and fails on an
email address outside the reserved domains (`example.com`, `.test`, ...), a
Windows domain SID other than the synthetic one, a tar member owned by
anyone but root, or a gzip header carrying a file name or timestamp.

A test that navigates to a specific target uses a **canonical**
ref (`#@cat:<id>/wl:<id>/ver:<uid>`, optionally `/device:<id>/object:<id>`)
or another stable catalog identifier (`workload_id`, `version_id`,
`stream_uuid`, `connection_config_id`): anonymization never touches these,
so the ref stays valid when a fixture is re-recorded, where a display-name
path would not. Human-ref parsing belongs in a synthetic unit test
(`tests/unit/cli/test_cli_commands_tree.py`, `test_cli_commands_ls.py`,
`tests/unit/sdk/test_units_node_ref.py`).

**A replay test checks an anonymized field structurally, or against another
value fetched in the same session** — any field the anonymization scrubs
(a connection/workload `display_name`, a device's `host_name`-derived name,
a SaaS site/team/group name, ...); a synthetic unit test covers the logic
with a fake name. Prefer a structural check (row/line count, shape) to a
negative one (`not in`, `!=`) against a placeholder string: placeholder
text can change when a fixture is re-recorded. Fields it leaves alone may be asserted literally: a version's
`display_name` (a timestamp, see "Timezone-rendered assertions"), the fixed
`gws_domain` `gwsdemo.example.com`, `subtitle`, `workload_type`,
`sub_type`, disk content names.

**Sample constants live in `tests/support/recording/sample_constants.py`**
— a sample's own generated artifacts that replay tests need verbatim, not
customer data. Replay tests import them from there; a new one gets a `#:`
line naming its sample's alias. `tests/unit/` uses
synthetic values (e.g. a hand-built `"AliceKey0001@<base64>"` key string).

## Disk image fixtures

`tests/fixtures/tiny_*.raw.gz`/`tiny_*.raw.tar.gz` are small,
purpose-built ext4/XFS/Btrfs/NTFS/APFS disk images, real bytes but no
repository data (nothing recorded or anonymized), so the
`tests/unit/sdk/test_units_content_disk_fs*.py` files that parse them with
`dissect.*` are unit tests. Each image's build recipe and storage format
(plain gzip, or a sparse tar for the larger ones) is in the docstring of the
module that uses it; `tests/unit/sdk/disk_fs_fakes.py`'s `raw_image`/
`extracted_image` load one at most once per test process.

## Timezone-rendered assertions

A version's `display_name` and the browser's date cells are rendered in the
local timezone by the SDK's `format_timestamp`. `tests/conftest.py`'s
session-scoped `_fixed_timezone` pins that to `Asia/Taipei` on every
platform, so a test may hardcode a rendered timestamp — check a new one
against the Asia/Taipei rendering, not your own machine's.

## Rendered-output assertions

A synthetic test asserts rendered output — a CLI command's human or
`--json` text, a presentation helper's string — whole, with an inline
snapshot: `assert result.stdout == snapshot()`. `uv run pytest
--inline-snapshot=create <files>` fills an empty one and `--inline-snapshot=fix`
updates a changed one, both run without `-n`; every other run, xdist and CI
included, fails on a mismatch. Read each written snapshot in the diff before
committing: it becomes the spec, so a wrong-looking render is a bug to report
or fix, not a value to accept. Keep a volatile part out of the render (run
from `tmp_path` and pass a bare file name, as `test_cli_commands_dump.py` does) or
assert that one fragment on its own. A replay test asserts structure
instead (see "What a replay test asserts").

## Driving a `Pilot` test: wait for the state, not for a duration

A `Pilot` test waits for the state a step needs with `tests/support/pilot.py`'s
`wait_until` (or a helper built on it: `wait_for_screen`, `focus_widget`,
`move_cursor_to`, `wait_for_detail_content`, `wait_for_filter_closed`). A
fixed `pilot.pause(n)` between an action and a read passes on an idle machine
and flakes on a busy one.

A test opens its screen through the driver that already waits for it:
`unit/browser/browse_screen_fakes.py`'s `open_browse_screen()`,
`unit/browser/connect_dialog_drivers.py`'s `open_connect_dialog()`, or, over a
replayed repository, `integration/browser/pilot_drivers.py`'s
`open_browser_pilot()`. An app-level test runs at `RUN_TEST_SIZE`.

Wait explicitly for three preconditions that are easy to assume: focus
(`focus_widget`), cursor placement (`move_cursor_to`), and a rebuilt tree
(`d`/`r` rebuild the whole tree, so re-acquire any `TreeNode` picked before
them).

Use `UI_TIMEOUT` (`wait_until`'s default) for a synchronous UI change
(`push_screen`, a widget attribute) and `SDK_TIMEOUT` for anything gated on
an SDK/Store/provider dispatch, even a fast one. To observe a transient
state (a loading indicator), hold the operation behind it open until the
test has seen it (see `test_browser_app_hex_filter_refresh.py`'s
loading-indicator tests).

To land a step between Textual removing a screen's widgets and its
`on_unmount`, use `step_until_pruned`.

To assert that something *never* happens, wait for the point at which it
would have happened, then assert: `settle(pilot)` when only queued messages
could cause it, `wait_for_workers` when a worker's late result could, and for
a timer (a debounce, a spinner tick) a shortened interval
(`fast_browser_debounce`, a monkeypatched module constant) plus a wait until it
has fired (`count_progress_ticks`).

## Closing a provider built directly against a repository

A test that constructs a `UnitProvider` directly (`RawObjectProvider.create()`,
`DeviceProvider.create()`, `FsProvider(...)`, `saas_provider_for()`, ...)
rather than through `Catalog.provider()` (whose providers the `Repository`
closes) owns its `SqliteSource`/`aiosqlite` connection and closes it under
`async with`, as for every `ClosableUnitProvider` (the SDK README's Design
Conventions). A helper that returns a still-open provider leaves closing to
its caller.

A `monkeypatch.setattr(obj, "close", ...)` that simulates a close failure
also stops the real cleanup: have the replacement delegate to the original
close, or close the real resource in the test's own `finally`.

## Async tests

`asyncio_mode = "auto"` (root `pyproject.toml`) runs `async def test_...()`
with no marker. A test-local fake `ObjectStore` subclasses
`support.store_fakes.WrappingStore` (or another fake there) where it can;
one written from scratch defines all five methods
(`read`/`size`/`exists`/`listdir`/`close`) as `async def` and carries
`@faithful_to(ObjectStore)`, which rejects a sync one —
`@runtime_checkable`'s `isinstance()` checks only that the methods exist.

A test orders concurrent steps with a gate, never a duration: an
`asyncio.Event` (a `threading.Event` for a worker thread) the fake sets when
it reaches a point or waits on until the test releases it, an `Event()`
nobody sets for a call that never returns, and an injected clock where the
code under test measures elapsed time.

Some browser Pilot tests are a sync `def test_...()` around
`asyncio.run(scenario())`, so the scenario owns one event loop for the whole
`App.run_test()` lifetime, independent of pytest-asyncio's loop and fixture
teardown. Either form is fine for a new test; keep a file consistent.

## Running the suite

`make test` is the pre-commit gate (see `CONTRIBUTING.md`'s "Before every
commit"). `make test-fast` (pytest only), `make test-quick` (pytest
without the Textual `Pilot` tests), `make test-unit` and
`make test-integration` are quicker loops; every pytest run replays the
committed `tests/integration/` fixtures, and re-recording one is a
separate step (see "Detects call-sequence drift, not semantic drift"). `tests/conftest.py` marks every test in a
module that drives a `Pilot` (itself or through a `tests/` module it
imports) with `pilot`, so `-m pilot` selects only those.

See [`CONTRIBUTING.md`](../CONTRIBUTING.md) for commit message conventions.
