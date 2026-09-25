# `synology_apm_repo.browser` — design conventions

See [`ARCHITECTURE.md`](../../../../../ARCHITECTURE.md) (repository root)
first — layering, presentation, and the facade contract are all there,
not repeated here. This document covers TUI-specific screen/worker
conventions and the "adding a new screen" recipe.

The TUI talks to the SDK's `Session`/`Repository`/`Catalog` facade — it
depends on `synology-apm-repo-sdk`, never on `synology-apm-repo-cli` — with
a few explicitly-named exceptions, for node-navigation/presentation
helpers with no Repository-layer equivalent: `UnitScreen` alone reaches
into `sdk.units.resolve` directly (goto-ref chain walking), and both `UnitScreen` and
`core/unit/select.py` reach into `sdk.units.saas.site` for its SharePoint
List-overview detection (a purely presentational tree-spec/navigation
decision, not a Repository-layer concern). See `ARCHITECTURE.md`
for the facade contract, `NodeRef` format, and the Presentation principles
below.

## MVU: `core/`, `runtime/`, `view/`

Three layers sit below `screens/`/`widgets/`, in strictly one direction
(`core` → `runtime` → `view` → `screens`/`widgets`, never back up):

- **`core/`** is pure — no Textual import at all, enforced by
  `tests/unit/browser/test_browser_core_no_textual_import.py`'s own `ast`
  walk of the real source tree, not just convention. `core/app/`,
  `core/unit/`, and `core/browse/` are this package's three full
  Model/Msg/Cmd/`update()` four-pieces. `core/app/`: `AppModel` (jobs,
  recent outcomes), `AppMsg` (`StartExport`, `ExportProgressed`, …),
  `AppCmd` (`RunExport`, `CancelGroup`, `Notify` — data, never a
  callable, which is what makes asserting on `update()`'s returned `cmds`
  a one-line test with no Pilot involved), and `update(model,
  msg) -> (model, cmds)` — pure, synchronous, exhaustive (`case _:
  assert_never(msg)`, so a new unhandled `Msg` is a mypy error, not a
  silent no-op). `core/unit/` drives `UnitScreen`'s own tree-navigation
  state the identical way: `UnitModel` (provider lifecycle, every
  expanded node's loaded/paginated children, the active filter),
  `UnitMsg`/`UnitCmd`, and `update()` — plus `select.py`, a pure
  `UnitModel -> NodeSpec[NodeRef]` projection consumed by
  `view/reconcile.py` (below), a role `core/app/` has no counterpart for
  since it drives no `Tree`. `core/browse/` drives `BrowseScreen`'s own
  three-column state the same way, but leans on a fourth primitive
  `core/unit/` doesn't need: `core/remote_data.py`'s `RemoteData`
  (`NotAsked`/`Loading[T]`/`Success[T]`/`FailureInfo`) is what
  `BrowseModel.repos`/`.catalog_workloads`/`.workload_versions` hold
  instead of a bare optional/dict-presence check, since a repository's
  own catalogs, a catalog's own workloads, and a workload's own versions
  each independently need their own "not asked yet, loading, loaded,
  failed (and why)" state rather than sharing one screen-wide notion of
  it the way `UnitModel.loaded`'s single dict already sufficed for.
  `core/browse/select.py` projects two trees (`catalog_tree_spec`,
  `workload_tree_spec`) plus one non-tree projection (`version_rows`,
  for column 3's `DataTable` — see `view/`'s own bullet below for why
  that one isn't a `NodeSpec`). Two more primitives are shared across all
  three domains, not just `core/browse/`'s: `core/keys.py`'s `is_stale()`
  is the one place every fetch-result case's own `epoch`/`request` check
  lives, and `core/notify.py`'s `Notify` is the one shared toast `Cmd`
  every domain's own `Cmd` union re-exports — reach for both before
  hand-rolling either again. `core/connect/validate.py` is a
  narrower kind of `core/` module: not a Model/update() four-piece, just
  pure per-field validation functions (`validate_local`/`validate_s3`/…)
  — a screen's own logic earns a full four-piece only when it actually
  has a `Store` to drive; `ConnectDialog` doesn't (see "Not every screen
  needs the four-piece" below), but its validation logic is exactly the
  kind of branchy, Textual-free logic `core/` exists for regardless.
- **`runtime/`** may import Textual. `store.py`'s `Store[Model, Msg, Cmd]`
  is the dispatch loop: `dispatch(msg)` drains a queue through `update()`,
  notifies slice-diffed subscribers *once* per drained batch (not once per
  message — `_notify()` runs before `_perform()`, so the UI shows
  `Loading` before an effect's own `run_worker` even starts), then performs
  every returned `Cmd`. `app_effects.py`'s `AppEffects.perform()`/
  `unit_effects.py`'s `UnitEffects.perform()`/`browse_effects.py`'s
  `BrowseEffects.perform()` are where each screen's own `Cmd` values
  actually turn into `run_worker` calls — `RunExport` → a real export
  worker, `CancelGroup` → `workers.cancel_group()`, `LoadRoot`/
  `LoadChildren` → a provider fetch dispatching `RootLoaded`/
  `ChildrenLoaded` back in, `LoadCatalogsFor`/`LoadWorkloads`/
  `LoadVersions` → the equivalent repository/catalog fetches dispatching
  `CatalogsLoaded`/`WorkloadsLoaded`/`VersionsLoaded` back in, `Notify` →
  `self._app.notify()`/`self._screen.notify()`. `resources.py`'s
  `ResourceTable` (with `core/keys.py`'s own `RepoHandle`/
  `ProviderHandle`) is the answer to "a `Repository`/`UnitProvider` holds
  a real `aiosqlite` connection, so it can never live in a frozen
  `Model`": the real object stays here, a `Model` holds only the opaque
  handle. Both halves are load-bearing now — `UnitModel.provider` holds a
  `ProviderHandle`, dereferenced only inside `UnitEffects`; `BrowseModel`
  holds one `RepoHandle` per discovered repository (`RepoState` is a
  handle plus a presentation-relevant snapshot, never the real object,
  since a real `Repository` owns an `aiosqlite` connection that can't
  live in a frozen model), dereferenced only inside
  `BrowseEffects`. Route a *new* `Store`-backed screen's own closable
  resources through `ResourceTable` the same way, rather than holding
  the real object directly on the screen.
- **`view/`** may import Textual. `reconcile.py`'s `reconcile_children(
  parent, specs)` updates one `Tree` level in place, keyed by domain
  identity (`NodeSpec.key`) rather than position — a survivor keeps its own
  `TreeNode` object (so its own expansion/cursor state survives too)
  instead of the level being destroyed and rebuilt on every keystroke of a
  filter. A bulk-removal fallback (clear and re-add every child) takes over
  when more than half of a level's managed children would be removed, since
  one-at-a-time removal costs one `Tree` invalidation per call while the
  fallback costs a fixed few for the whole batch. No `DataTable` equivalent
  exists: `DataTable` can't insert a row at a position or add a column
  without a running `App`, unlike the `Tree` methods this reconciler calls.
  `reconcile_and_restore_cursor(
  tree, specs)` is the actual entry point every screen's own render method
  uses (`UnitScreen._render_tree`/`BrowseScreen._render_catalog_tree`/
  `_render_workload_tree`): it reconciles the whole tree from the model's
  fresh `NodeSpec`s (`core/unit/select.py`'s `folder_tree_spec`, `core/browse/
  select.py`'s `catalog_tree_spec`/`workload_tree_spec`) in one call, then
  restores the cursor by domain key rather than trusting a survivor's
  object identity to also mean "the cursor followed it" (`Tree.cursor_node`
  is derived from `cursor_line`, a line *position*, not a node's identity).
  When the cursor's own key didn't survive, it falls back to the nearest
  surviving sibling, then climbs the cursor's own ancestor chain, then the
  tree's own root as a last resort.
  `UnitScreen`'s own tree is whole-tree-uniform (its root really is
  a domain `Node`, wrapped in a `Binding` too, not special-cased, so
  `update_node()` is applied to it directly); `BrowseScreen`'s two trees
  each have a permanent, non-domain root ("Catalogs"/"Workloads") whose
  shape is fixed, so there's nothing to
  wrap at the root itself, only its children. `BrowseScreen`'s own
  workload-tree keys (`core/browse/select.py`'s `WorkloadGroupKey`) have
  to be globally unique across the *whole* tree, not just among one
  level's own siblings, since `reconcile_and_restore_cursor`'s own
  cursor-restore index spans every level at once — a bare `type_hint`
  string (which two different SaaS tenants' own sub_type groups can
  share) isn't enough on its own; `WorkloadGroupKey.path` encodes the full
  ancestor chain instead. A new screen with a `Tree` level that both
  filters and needs to preserve cursor/expansion state across that filter
  is exactly what this was built for — reach for it before inventing a
  second reconciler.

### Not every screen needs the four-piece

`core/app/*` exists because `ExportScreen`'s own background jobs
genuinely need to keep running, and stay visible in `WorklistScreen`'s own
status bar, after the screen that started them is popped — state that
outlives any one screen is exactly what an app-level `Store` is for.
`core/unit/*`/`core/browse/*` exist for a different reason each: both
`UnitScreen`'s and `BrowseScreen`'s own tree-navigation state needs a
race-free staleness check across concurrent fetches and no `id(TreeNode)`-
reuse hazard, which a pure `update()`'s exhaustive `epoch`/`Slot`-keyed
staleness check and `view/reconcile.py`'s keyed reconciler close
structurally rather than call-site by call-site — a `Store` scoped to
each one screen, not the app-level one, is what pays for itself here.
Every screen that had this shape of problem now has a `Store`; **no
screen in this package currently holds an `id(TreeNode)`-keyed cache or
a hand-rolled epoch counter outside one** — that state lives entirely
inside `core/unit/update.py`'s/`core/browse/update.py`'s own `Model`
fields instead (`UnitModel.loaded: Mapping[NodeRef, LoadedLevel]`,
`BrowseModel.repos`/`.catalog_workloads`/`.workload_versions`, each
`epoch`/`Slot`-checked the same way). Most other screens have neither
problem, and forcing a `Store` onto them anyway is explicitly *not* the
goal here — `HelpScreen`, say, has nothing that survives its own lifetime
worth a `Model` for, and `ConnectDialog`'s own complexity is async
workflow (scan progress, profile CRUD, remote browsing), not a `Tree`
with a race to close, so `core/connect/validate.py` stays the narrower
pure-functions shape instead (see the `core/` bullet above). What "every
screen is MVU" actually means in this package: every screen either
dispatches `Msg`s into a real `Store` (the shared app-level one, or its
own), or — just as legitimately — keeps its state as plain instance
fields, reaching for the two disciplines below the moment a genuine race
or `id(TreeNode)` hazard actually shows up before that state grows into
a full `Store`:

- **A `Cmd`-shaped race still needs a `Cmd`-shaped fix even with no
  `Store` in sight** — the "capture at dispatch, check again right
  before publish" shape `core/unit/update.py`'s/`core/browse/update.py`'s
  own `epoch`/`Slot` checks now use for every fetch-result `Msg` (or
  `core/app/update.py`'s own `ExportProgressed` case, checking whether
  `job_id` still exists in `model.jobs` before applying a late progress
  tick) is exactly what a screen without a `Store` would inline directly
  as a plain counter field instead, bumped on every fresh
  selection/reset and checked again once the fetch itself resolves.
  `exclusive=True`/a cancelled worker are never trusted alone for this:
  cancellation can't stop a worker that's already past its final `await`
  from finishing and publishing anyway, so the
  counter check, not the cancellation, is what actually prevents a stale
  result winning. Reach for it directly on a new screen's own instance
  fields only if that screen's complexity doesn't yet warrant a full
  `Store`, and revisit that call once it does.
- **Bookkeeping that a `Tree`'s own destroy/recreate cycle touches is
  keyed by domain identity, never `id(TreeNode)`** — the same reasoning,
  now similarly Store-internal on every screen that needs it (see above)
  rather than a plain instance-field dict: CPython can and does reuse a
  destroyed object's address for an unrelated later one, so a domain
  key (a `NodeRef`, a `WorkloadKey`, ...) is what survives a filter
  narrow-then-widen round trip's own already-fetched data intact instead
  of forgetting it on a rebuild. A cache keyed by a node that's never
  destroyed independently of the whole level being torn down has no such
  hazard to guard against in the first place, and can stay `id()`-keyed
  safely — a *reconciled* level's survivors need no such cache at all,
  since reconciliation never destroys a survivor to begin with.

### View-local state never needs a `Cmd`

Not everything a screen touches belongs in a `Model`, `Store`-backed or
not — reactive UI mechanics that exist purely to make the *widget* behave
correctly, with no domain meaning of their own, are mutated directly, in
place, with no dispatch:

- A `Debouncer`'s own pending-fire timer (`widgets/filter_debounce.py`) —
  dispatching once per keystroke just to track "is a debounce pending"
  would turn a `Store`'s own slice-diffed subscription model against
  itself (2 renders/sec while an animation/spinner runs, say) for zero
  domain benefit.
- `DebouncedProgress`'s own spinner frame, wherever it's actually rendered
  (`TreeNodeLoadingSink`/`DataTableLoadingRowSink`/`StaticTextSink`/the
  default breadcrumb `_LoadingSink`) — same reasoning, doubled: a `Model`
  field mutated on a timer is the textbook way to turn a `Store`'s "one
  render per drained batch" guarantee back into "one render per tick,"
  undoing the exact thing `Store._drain()` exists to fix.
- `Input.value`/`Checkbox.value`/`Select.value` — never mirrored into a
  `Model`. Two reasons, not one: a `Model` field and the widget's own
  cursor/selection state would fight over which is authoritative on every
  keystroke, and — the harder rule — **a secret never enters a `Model`,
  full stop.** `ConnectDialog`'s own S3 secret key/Azure credential/SMB
  password are read from their `Input`s, passed straight through
  `core/connect/validate.py`'s own pure functions into
  `store_from_config()`, and held nowhere else — not because a rule
  forbids storing them, but because there's no `Model` for them to enter
  in the first place. A screen that *does* have a real `Store` follows the
  same discipline: nothing secret-shaped is ever a `Model` field, even
  transiently.
- A `Tree`'s own cursor position, an expanded/collapsed flag Textual
  itself already tracks on the `TreeNode` — reconciliation (`view/
  reconcile.py`) exists precisely so a survivor's own `TreeNode` keeps
  these without either being copied into any `Model` at all.

## Screen and Worker Conventions

- **Every background-ish operation is a native async worker (`@work` or
  `run_worker`), never `thread=True`.** An `async def` method decorated
  `@work` (no `thread=True`) runs as a real `asyncio.Task` directly on the
  app's own event loop. This means no
  `self.app.call_from_thread(...)` marshaling is needed anywhere: you're
  already on the right thread/loop, so call things directly.
  `@work`'s own `group=` is fixed at decoration time; a worker that needs a
  *per-dispatch* group (one export job's own cancel group, say) uses
  `DOMNode.run_worker(functools.partial(...), group=..., exit_on_error=False)`
  instead — `functools.partial`, never a lambda wrapping a coroutine:
  Textual's own `Worker._run_async` checks
  `inspect.iscoroutinefunction(self._work)`/`.func`, which a bare lambda
  never satisfies — only wrapping the real async method in a `partial`
  exposes it there. Host matters too:
  `self.run_worker(...)`/`@work` on a
  screen is cancelled automatically the instant that screen unmounts, so a
  worker that must outlive the screen that started it (closing a
  `UnitProvider`'s own connection, or a discarded `Repository`'s, say) is
  hosted on the App instead (`self.app.run_worker(...)`, never `self`):
  a worker hosted on the screen dies with it (`Widget._on_unmount` cancels
  every worker on that node), which would cancel the close itself out from
  under it — exactly the leaked-connection failure mode closing a provider
  exists to prevent. That
  pattern is used by `UnitEffects.perform`'s `CloseProvider` case/
  `UNIT_PROVIDER_CLOSE_GROUP` and `runtime/browse_effects.py`'s
  `BrowseEffects.perform`'s `CloseRepos` case/`BROWSE_REPO_CLOSE_GROUP`;
  `app.py`'s own `on_unmount` *drains* (never cancels) both groups before
  closing the session.
- **A `screens/*.py` worker method imports `work` from
  `widgets/worker_progress.py`, never `textual` directly** (enforced by
  `tests/unit/browser/test_browser_screens_use_tracked_work.py`'s own ast
  walk, the same technique `test_browser_core_no_textual_import.py` uses
  for a different rule). It's a drop-in replacement for `textual.work`
  that additionally wraps the decorated coroutine's body in
  `DebouncedProgress` by default — showing *some* busy indicator is the
  default, opt-out behavior, not something a call site has to remember to
  add. `runtime/*_effects.py`'s `perform()` gets the same
  treatment from `run_worker_with_progress`/`run_worker_no_progress`
  (an `Effects` instance's own `self` is never a `Widget`, so
  `textual.work` doesn't apply there — these wrap the same
  `host.run_worker(...)` call by hand instead), enforced by
  `test_browser_effects_use_progress_helpers.py`. `busy=False`/
  `run_worker_no_progress` is the explicit opt-out — for a worker that
  delegates its entire body to an already-wrapped helper (wrapping again
  would double the same sink's own debounce/animation state), or a
  background operation with its own dedicated progress UI already
  (`AppEffects.perform`'s `RunExport` case, whose progress lives in
  `AppModel.jobs` instead) — always with a comment at the call site
  explaining why, since that judgment isn't something either test can
  make for you.
- **Anchor a loading indicator at the specific widget that is about to
  display the new content — never default to the screen-wide breadcrumb
  because nothing else was wired up.** The breadcrumb (`work`/
  `run_worker_with_progress`'s default sink) is reserved for the one case
  where the screen's own location/identity is what's changing (a `g`
  jump, or a transient resolve before pushing an entirely different
  screen) — a workload/version fetch that populates a *different* column
  than the one clicked belongs on that column's own anchor instead
  (`TreeNodeLoadingSink` on a tree's own root for a `Tree` column with
  no other node to target, `DataTableLoadingRowSink` for a flat
  `DataTable` with no per-row equivalent, `StaticTextSink` for a
  Store-less screen's own single status line). This is a placement
  judgment neither enforcement test above can check mechanically — code
  review is what catches "the right sink exists, but points at the wrong
  widget."
- **Long operations must not block the UI for more than ~300ms without
  feedback.** Use the existing debounced-progress pattern
  (`widgets/progress_hint.py`) rather than inventing a new one — it already
  handles "don't flash a spinner for something that finishes instantly."
- **`Esc` cancellation must react within 200ms** (`dedup/chunk_walk.py`'s
  `_MAX_MERGED_RUN` docstring has the reasoning — a single merged write is
  capped so it can't block a cancellation past that budget) — if a new screen
  adds a cancellable operation, make sure cancelling it actually reaches an
  `await` point quickly; don't wrap a long stretch of non-cooperative work in
  a single `to_thread()` call with no way out.
- **Verbose content (`d` key, matching the CLI's `--verbose`) is opt-in** —
  same rule as `ARCHITECTURE.md`'s Presentation section, applied to the TUI:
  internal identifiers, `NodeRef` canonical form, hex previews, and verify
  findings all stay behind the verbose toggle. One deliberate exception: an
  `ApmRepoError`'s own displayed detail (`str(exc)`, `ref=`/`spec=` tags
  included) is never gated — don't add a `d`-toggle branch there, even
  though the surrounding rule might suggest one.
- **Modal screens (`KeyDialog`, `ExportScreen`, `WorklistScreen`) are
  `ModalScreen[T]`, centered, not full-screen replacements.** Only
  genuinely full-browsing screens (`BrowseScreen`, `UnitScreen`,
  `DiagnosticsScreen`, `HexPreviewScreen`) replace the whole viewport. Check
  which shape a new screen actually is before picking a base class. Not
  every modal is a one-shot form, though: `KeyDialog`/`ConnectDialog` are,
  but `ExportScreen`/`WorklistScreen` show a working screen's own ongoing
  state (a running job's progress, the live jobs list) in the same centered
  treatment, which costs an explicit delegate `action_*` method per binding
  on a modal (`COMMON_BINDINGS` needs those to actually work, not just the
  `Binding` itself).
- **ETA/rate/byte-size formatting comes from the SDK's `presentation/`
  module** (`ProgressMeter`, `format.py`) — same rule as
  `ARCHITECTURE.md`'s Presentation section: never recompute or reformat
  these independently in a screen.
- **`y` copies a canonical `NodeRef`; `g` navigates to one.** A new screen with
  navigable content should support both if it makes sense to bookmark/jump to
  a node on it — don't invent a parallel addressing scheme.

## Adding a New Screen

1. Decide full-screen vs. modal (see above) and pick the matching base class.
2. If it needs SDK data, fetch it in an async `@work` method (imported from
   `widgets/worker_progress.py`, never `textual` directly — see "Screen and
   Worker Conventions" above; no `thread=True`), never inline in
   `compose()`/`on_mount()` synchronously for anything that does real I/O.
   Decide where its busy indicator belongs (the placement rule above) —
   the default breadcrumb sink is rarely the right answer once the screen
   has more than one region.
3. Decide whether it needs a real `Store` at all (see "Not every screen
   needs the four-piece" above) — state that must outlive this screen's own
   lifetime (an app-level `Store`, `core/app/*`'s own reason to exist) or
   state whose races/`id(TreeNode)` hazards a hand-rolled epoch counter
   would only patch call-site by call-site (a screen-scoped `Store`,
   `core/unit/*`'s/`core/browse/*`'s own reason) is the deciding question,
   not "does it have any state." If either applies, its
   `Model`/`Msg`/`Cmd`/`update()` go under `core/<screen>/`, never inline
   in the screen module. If neither does, plain instance fields are fine
   — just apply the epoch/domain-key disciplines above wherever a race or
   an `id(TreeNode)` cache would otherwise sneak in.
4. Respect the verbose-mode gate for anything showing an internal
   identifier.
5. Add a Pilot test. A screen that needs real SDK/sample data belongs under
   `tests/integration/browser/test_browser_pilot*.py` — an integration test
   backed by a recorded fixture, see
   [`tests/CLAUDE.md`](../../../../../tests/CLAUDE.md)'s "RecordingStore /
   ReplayStore" section. A screen needing no SDK/fixture data at all (e.g.
   `HelpScreen`, whose own test is pure bindings introspection) gets a
   `tests/unit/browser/` Pilot test instead — still real Textual-`Pilot`-
   driven, just without the fixture machinery. Either way, static review
   alone doesn't catch real Textual behavior (e.g. `Tree.move_cursor`
   needing a forced rebuild) — don't skip writing one because the screen
   "looks simple." A screen with a real `Store` additionally gets its own
   pure `core/<screen>/update.py` tests, no Pilot needed for those branches
   at all — see `test_browser_core_app_update.py` for the shape.
6. Wire any new keybinding into `keymap.py` and check for collisions: a key
   bound in two `BINDINGS` lists silently loses to whichever Textual resolves
   first, with no error and no visible signal beyond "the key doesn't seem to
   do anything."

## Package Layout

One module per screen under `screens/`, and per reusable widget under
`widgets/`. `app.py` owns the `App` and session lifecycle;
`screens/key_dialog.py`'s `KeyDialog`, pushed from `BrowseScreen`, owns
key-material prompts; `keymap.py` is the source of truth for keybindings;
other root-level modules (`content_preview/`, `list_overview.py`,
`workload_grouping.py`, `repo_labels.py`, `strings.py`) are pure
presentation helpers shared across screens. `core/`/`runtime/`/`view/`
hold the MVU layers described above — see that section for what belongs
in each.
