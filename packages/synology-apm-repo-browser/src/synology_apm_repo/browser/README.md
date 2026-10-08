# `synology_apm_repo.browser` — design conventions

See [`ARCHITECTURE.md`](../../../../../ARCHITECTURE.md) (repository root)
first — layering, presentation, and the facade contract are all there,
not repeated here. This document covers TUI-specific screen/worker
conventions and the "adding a new screen" recipe.

## MVU: `core/`, `runtime/`, `view/`

Three layers sit below `screens/`. `view/` imports nothing else in the
package; `core/` uses it only for `view/reconcile.py`'s `NodeSpec` data
values (in `core/*/select.py`); `runtime/` builds on `core/`, `view/` and
`widgets/`, which itself uses only the leaves; `screens/` sit on top.
`strings` and `content_preview/` are leaves every layer but `view/` may
use. Imports never point back up
(`scripts/check_browser_layers.py`, run by `make lint`, checks the source
tree, `TYPE_CHECKING` imports included):

- **`core/`** is pure and imports no Textual module itself
  (`tests/unit/browser/test_browser_core_no_textual_import.py` checks the
  source tree). `core/app/`, `core/unit/`, and
  `core/browse/` are this package's three Model/Msg/Cmd/`update()`
  four-pieces, one per screen (or app-level) that needs a `Store` — see
  "Not every screen needs the four-piece" below for when a screen doesn't.
  Each `update()` is one exhaustive `match` (`assert_never` in its last
  case); a message whose handling runs past a few lines gets its own
  `_on_<message>(model, msg)` function, which that case only returns.
  `core/keys.py`'s `is_stale()`, `core/notify.py`'s `Notify` and
  `core/text_filter.py`'s `matches_filter()` (every `/` filter's rule) are
  shared across all three. `update()` decides whether a request is
  redundant (already loaded, already in flight) and makes it a no-op, and
  whatever a screen renders that derives from the model (filtered rows, the
  breadcrumb) is a `core/*/select.py` selector, so a screen dispatches and
  renders without re-deriving either; it keeps only checks on its own
  widgets. `core/connect/validate.py` holds plain validation functions for
  a screen with no `Store`, and `runtime/connect.py` their I/O counterpart
  (building the store, scanning it), so `ConnectDialog` keeps only its
  widgets.
- **`runtime/`** may import Textual. `store.py`'s `Store[Model, Msg, Cmd]`
  is the dispatch loop: drains messages through `update()`, notifies
  subscribers once per drained batch, then performs every returned `Cmd`.
  Each `Store`'s `*_effects.py` turns its `Cmd` values into workers. A
  frozen `Model` refers to a live, closable SDK object (a `Repository`, a
  `UnitProvider`) by a `RepoHandle`/`ProviderHandle` (`core/keys.py`); the
  object itself lives in `resources.py`'s app-wide `ResourceTable` and is
  dereferenced only inside effects. Route a new `Store`-backed screen's
  closable resources through it the same way.
- **`view/`** may import Textual. `reconcile.py`'s `reconcile_children()`
  updates a `Tree` in place, keyed by domain identity, so a survivor keeps
  its `TreeNode` (and its expansion/cursor state) across, say, every
  keystroke of a filter. Screens render through
  `reconcile_and_restore_cursor()`, which also restores the cursor by
  domain key, since `Tree.cursor_node` follows line position, not node
  identity. A domain key must be unique across the *whole* tree, since
  cursor restore spans every level.

### Not every screen needs the four-piece

`core/app/*` exists because export jobs outlive the screen that started
them. `core/unit/*`/`core/browse/*` exist because their tree-navigation
state needs a race-free staleness check and no `id(TreeNode)`-reuse
hazard; `core/unit/*` also holds `UnitScreen`'s detail pane content (the
selected node, its preview/overview, one request at a time), so a late
fetch for a node the user has left is dropped like any stale result. Most
other screens have neither problem — `HelpScreen` and
`ConnectDialog`, for instance, keep plain instance fields instead. What
"every screen is MVU" means in this package: every screen either
dispatches `Msg`s into a real `Store`, or keeps plain instance fields and
applies these two disciplines directly the moment a race or identity-reuse
hazard actually shows up:

- **A `Cmd`-shaped race still needs a `Cmd`-shaped fix even with no
  `Store` in sight** — capture a counter at dispatch, check it again right
  before publishing a late result. `exclusive=True`/a cancelled worker
  alone isn't enough: a worker already past its final `await` can still
  finish and publish anyway.
- **Bookkeeping a `Tree`'s destroy/recreate cycle touches is keyed by
  domain identity, never `id(TreeNode)`** — CPython can reuse a destroyed
  object's address for an unrelated later one. A cache keyed by a node
  that reconciliation never destroys has no such hazard.

### View-local state never needs a `Cmd`

Reactive UI mechanics with no domain meaning of their own are mutated
directly, no dispatch: a `Debouncer`'s pending-fire timer, a progress
spinner's frame, `Input`/`Checkbox`/`Select` values (never mirrored into a
`Model` — and a secret, e.g. a credential typed into `ConnectDialog`, never
enters a `Model`, full stop, even transiently), and a `Tree`'s own
cursor/expansion state (already preserved by reconciliation).

## Screen and Worker Conventions

- **Every background operation is a native async worker on the app's event
  loop (`@work` or `run_worker`), never `thread=True`.** A per-dispatch
  worker is `run_worker(functools.partial(...), group=...)`: a bare lambda
  fails Textual's coroutine-function check. Host a worker on `self.app`
  when it must outlive the screen that started it (closing a provider or
  repository, an export): a screen-hosted worker is cancelled when the
  screen unmounts.
- **A `screens/*.py` worker uses `work` from `widgets/worker_progress.py`,
  and a `runtime/*_effects.py` dispatch uses `run_worker_with_progress`/
  `run_worker_no_progress`** (enforced by ruff's `TID251` ban on
  `textual.work` and by `test_browser_runtime_effects_use_progress_helpers.py`). These show a
  debounced busy indicator by default, so a long operation always gives
  feedback after ~300 ms without flashing one for a quick call. Opt out
  (`busy=False`, `run_worker_no_progress`) only where the call site has its
  own progress UI or delegates to an already-wrapped helper, and say why in
  a comment.
- **Anchor a loading indicator at the widget about to show the new
  content** (`TreeNodeLoadingSink`, `DataTableLoadingRowSink`,
  `StaticTextSink`, `DetailLoadingSink`), including a fetch that fills a
  different column than the one clicked. Keep the default breadcrumb sink
  for a change of the screen's own location or identity (a `g` jump, a
  root load, a resolve before pushing another screen).
- **`Esc` cancellation reaches an `await` point quickly**: keep a
  cancellable operation's non-cooperative stretches short, the way the SDK
  bounds one merged chunk write (`_MAX_MERGED_RUN` in `dedup/chunk_walk.py`),
  rather than wrapping a long one in a single `to_thread()` call.
- **Verbose mode is `app.verbose`, toggled by `d`**; a screen watches it
  (`self.watch(self.app, "verbose", ...)`) and re-renders what
  `ARCHITECTURE.md`'s Presentation section gates behind it. A screen with a
  Store (`BrowseScreen`, `UnitScreen`) dispatches `VerboseSet` and lets its
  selectors read `model.verbose`, so its subscriptions re-render.
- **Full-browsing screens (`BrowseScreen`, `UnitScreen`,
  `DiagnosticsScreen`, `HexPreviewScreen`) subclass
  `screens/_shared/navigable_screen.py`'s `NavigableScreen`**, which
  supplies `j`/`k`/`l`/Enter forwarding, the Esc/`g`/`v` actions and the
  breadcrumb, `DebouncedProgress`'s "Loading" suffix included. The two
  with a `Store` (`BrowseScreen`, `UnitScreen`) subclass its
  `screens/_shared/store_screen.py` `StoreScreen` instead, which builds the
  store (`_open_store`) and owns the teardown order: close the store, then
  cancel and drain the screen's own workers, then `_after_store_closed`
  (`UnitScreen` closes its provider there).
  **Everything else (`ConnectDialog`, `KeyDialog`, `ExportScreen`,
  `WorklistScreen`, `HelpScreen`) is a centered `ModalScreen[T]`** mixing in
  `AppStateMixin` when it needs typed `app_state` access, whether a
  one-shot form or a live view of ongoing state (a job's progress, the jobs
  list). A modal's binding chain stops at itself, so a modal that keeps
  `COMMON_BINDINGS` also mixes in `DelegatesCommonActions`.
- **`r` refreshes by dropping the repository's caches, not by reopening it.**
  A refresh's load command (`LoadRoot`, `LoadWorkloads`, `LoadVersions`)
  carries `invalidate=True`, and its effect calls
  `Repository.invalidate_caches()` first; the `Catalog` the screen holds
  stays valid. That call replaces db connections, so nothing else may be
  reading the repository. `UnitScreen` cancels and drains its other workers
  before dispatching. Browse-screen loads can't be cancelled (a cancelled
  worker never dispatches, leaving its slot `Loading`), so every
  browse-screen effect that calls into a repository, and `UnitScreen`'s
  root load, holds the app-wide `ResourceTable.load_gate`: shared for a
  load, exclusive for the invalidation, which waits for running loads while
  new ones wait for it. An export holds the gate shared for its whole run,
  so `r` is refused with a warning while one runs.
- **Reconnecting (`c`, or `Esc` on the root screen) is refused while an export is
  running or queued**, because it closes every open repository, including the
  one the export reads from.
- **A provider is released through `ResourceTable.release_provider`**, which goes
  via `Repository.release_provider` so the repository stops tracking it.
- **A folder export opens its own provider** (`FolderExport` carries the
  `Repository`/`Catalog`/`Version`, not the `UnitScreen`'s provider): that screen
  releases its provider on navigating away while an export may still be queued
  or running. The TUI has no `--force`, so any existing destination file
  refuses the whole export before the first write.
- **`y` copies a canonical `NodeRef`; `g` navigates to one.** A new screen
  whose nodes are worth bookmarking supports both, through `NodeRef`.

## Adding a New Screen

1. Decide full-screen vs. modal (see above) and pick the matching base class.
2. Decide whether it needs a real `Store` (see "Not every screen needs the
   four-piece" above): state that must outlive the screen (`core/app/*`'s
   reason) or races/`id(TreeNode)` hazards an epoch counter would only patch
   call site by call site (`core/unit/*`'s and `core/browse/*`'s) decide it,
   not "does it have any state." If it does, put its
   `model.py`/`msg.py`/`cmd.py`/`update.py`/`select.py` under
   `core/<screen>/` and its effects in `runtime/<screen>_effects.py`; the
   screen subclasses `StoreScreen`, builds the `Store` with `_open_store`
   in `on_mount` and renders through `store.subscribe(selector, render)`;
   `StoreScreen` closes it on unmount. If not, keep plain instance fields and apply the
   epoch/domain-key disciplines above wherever a race or an
   `id(TreeNode)` cache would otherwise sneak in.
3. Fetch SDK data in a worker (see "Screen and Worker Conventions"), never
   synchronously in `compose()`/`on_mount()`, and decide where its busy
   indicator belongs.
4. Show internal identifiers only in verbose mode, escape repository-derived
   text with `sdk.presentation`'s `safe()`, and put static user-visible
   strings in `strings.py`.
5. Put a keybinding more than one screen shares into `keymap.py` (a
   screen-local one goes in that screen's own `BINDINGS`), give every new
   action an entry in `screens/help_screen.py`'s `_ACTION_CATEGORY`, and
   check a new key for collisions: when two `BINDINGS` lists bind one key,
   whichever Textual resolves first wins, silently.
6. Add a Pilot test. A screen that needs real SDK/sample data belongs under
   `tests/integration/browser/test_browser_screens_<screen>_<behaviour>.py`,
   backed by a recorded fixture (see [`tests/CLAUDE.md`](../../../../../tests/CLAUDE.md)'s
   "`RecordingStore` / `ReplayStore`" section); one that runs on fakes or no
   data at all gets a `tests/unit/browser/` Pilot test. Static review alone
   misses real Textual behavior (e.g. `Tree.move_cursor` needing a forced
   rebuild), so write one even for a screen that looks simple. A screen
   with a real `Store` also gets pure `core/<screen>/update.py` tests,
   shaped like `test_browser_core_app_update.py`.

## Package Layout

One module per screen under `screens/` (with `screens/_shared/` holding
`NavigableScreen`, `StoreScreen`, `AppStateMixin` and the helpers screens
share), and per
reusable widget under `widgets/`. `app.py` owns the `App` and session
lifecycle; `keymap.py` holds the keybindings screens share; `strings.py`
the static user-visible strings; `worker_drain.py` the bounded wait for
workers at teardown; `content_preview/` and `list_overview_table.py` are
pure presentation helpers. `core/`/`runtime/`/`view/` hold the MVU layers
described above — see that section for what belongs in each.
