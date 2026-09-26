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
  walk of the real source tree. `core/app/`, `core/unit/`, and
  `core/browse/` are this package's three Model/Msg/Cmd/`update()`
  four-pieces, one per screen (or app-level) that needs a `Store` — see
  "Not every screen needs the four-piece" below for when a screen doesn't.
  `core/keys.py`'s `is_stale()` and `core/notify.py`'s `Notify` are shared
  across all three. `core/connect/validate.py` is a narrower kind of
  `core/` module: pure per-field validation functions, not a four-piece —
  a screen earns a full four-piece only when it actually drives a `Store`.
- **`runtime/`** may import Textual. `store.py`'s `Store[Model, Msg, Cmd]`
  is the dispatch loop: drains messages through `update()`, notifies
  subscribers once per drained batch (not once per message), then performs
  every returned `Cmd`. Each screen's own `*_effects.py` turns its `Cmd`
  values into `run_worker` calls. `resources.py`'s `ResourceTable` is how a
  `Model` — which must stay frozen — holds a real closable resource (e.g.
  an `aiosqlite` connection): the real object stays in the table, the
  `Model` holds only an opaque handle, dereferenced only inside that
  screen's own effects. Route a new `Store`-backed screen's closable
  resources through it the same way.
- **`view/`** may import Textual. `reconcile.py`'s `reconcile_children()`
  updates one `Tree` level in place, keyed by domain identity rather than
  position, so a survivor keeps its own `TreeNode` (and its
  expansion/cursor state) instead of the level being rebuilt on every
  keystroke of a filter. `reconcile_and_restore_cursor()` is the actual
  entry point every screen's render method uses — it also restores the
  cursor by domain key, since `Tree.cursor_node` is derived from line
  position, not node identity. A domain key must be unique across the
  *whole* tree, not just one level, since cursor restore spans every level
  at once.

### Not every screen needs the four-piece

`core/app/*` exists because export jobs outlive the screen that started
them. `core/unit/*`/`core/browse/*` exist because their tree-navigation
state needs a race-free staleness check and no `id(TreeNode)`-reuse
hazard. Most other screens have neither problem — `HelpScreen` and
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

- **Every background-ish operation is a native async worker (`@work` or
  `run_worker`), never `thread=True`** — you're already on the app's event
  loop, no `call_from_thread` marshaling needed. A per-dispatch group uses
  `run_worker(functools.partial(...), group=...)` — a bare lambda doesn't
  satisfy Textual's coroutine-function check. Host a worker on `self.app`
  (not `self`) when it must outlive the screen that started it (e.g.
  closing a `UnitProvider`'s connection) — a screen-hosted worker is
  cancelled the instant the screen unmounts, which would cancel the close
  itself.
- **A `screens/*.py` worker method imports `work` from
  `widgets/worker_progress.py`, never `textual` directly** (enforced by an
  ast-walk test). It wraps the coroutine in `DebouncedProgress` by
  default, so a busy indicator is opt-out, not opt-in. `runtime/
  *_effects.py`'s `perform()` gets the same treatment via
  `run_worker_with_progress`/`run_worker_no_progress`. Opt out
  (`busy=False`) only when a call site already has its own progress UI or
  delegates entirely to an already-wrapped helper — with a comment
  explaining why.
- **Anchor a loading indicator at the specific widget that is about to
  display the new content — never default to the screen-wide breadcrumb
  because nothing else was wired up.** The breadcrumb is reserved for the
  one case where the screen's own location/identity is what's changing (a
  `g` jump, or a transient resolve before pushing a different screen) — a
  fetch that populates a different column than the one clicked belongs on
  that column's own anchor instead (`TreeNodeLoadingSink`,
  `DataTableLoadingRowSink`, `StaticTextSink`).
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
