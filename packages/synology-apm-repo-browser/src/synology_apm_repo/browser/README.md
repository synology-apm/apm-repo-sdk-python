# `synology_apm_repo.browser` — design conventions

See [`ARCHITECTURE.md`](../../../../../ARCHITECTURE.md) (repository root)
first — layering, presentation, and the facade contract are all there,
not repeated here. This document covers TUI-specific screen/worker
conventions and the "adding a new screen" recipe.

The TUI talks to the SDK's `Session`/`Repository`/`Catalog` facade — it
depends on `synology-apm-repo-sdk`, never on `synology-apm-repo-cli` — with
one explicitly-named exception: `UnitScreen` reaches into
`sdk.units.device_disk_fs`/`sdk.units.resolve`/`sdk.units.saas.site`
directly, for node-navigation helpers with no Repository-layer equivalent
(see `sdk/api/__init__.py`'s own docstring). See `ARCHITECTURE.md` for the
facade contract, `NodeRef` format, and the Presentation principles below.

## Screen and Worker Conventions

- **Every background-ish operation is a native async `@work`, never
  `@work(thread=True)`.** An `async def` method decorated `@work` (no
  `thread=True`) runs as a real `asyncio.Task` directly on the app's own event
  loop. This means no
  `self.app.call_from_thread(...)` marshaling is needed anywhere: you're
  already on the right thread/loop, so call things directly.
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
- **Modal, form-shaped screens (`KeyDialog`, `ExportScreen`) are
  `ModalScreen[T]`, centered, not full-screen replacements.** Only genuinely
  full-browsing screens (`BrowseScreen`, `UnitScreen`, `DiagnosticsScreen`,
  `WorklistScreen`, `HexPreviewScreen`) replace the whole viewport. Check which
  shape a new screen actually is before picking a base class.
- **ETA/rate/byte-size formatting comes from the SDK's `presentation/`
  module** (`ProgressMeter`, `format.py`) — same rule as
  `ARCHITECTURE.md`'s Presentation section: never recompute or reformat
  these independently in a screen.
- **`y` copies a canonical `NodeRef`; `g` navigates to one.** A new screen with
  navigable content should support both if it makes sense to bookmark/jump to
  a node on it — don't invent a parallel addressing scheme.

## Adding a New Screen

1. Decide full-screen vs. modal (see above) and pick the matching base class.
2. If it needs SDK data, fetch it in an async `@work` method (no
   `thread=True`), never inline in `compose()`/`on_mount()` synchronously for
   anything that does real I/O.
3. Respect the verbose-mode gate for anything showing an internal
   identifier.
4. Add a Pilot test. A screen that needs real SDK/sample data belongs under
   `tests/integration/browser/test_browser_pilot*.py` — an integration test
   backed by a recorded fixture, see
   [`tests/CLAUDE.md`](../../../../../tests/CLAUDE.md)'s "RecordingStore /
   ReplayStore" section. A screen needing no SDK/fixture data at all (e.g.
   `HelpScreen`, whose own test is pure bindings introspection) gets a
   `tests/unit/browser/` Pilot test instead — still real Textual-`Pilot`-
   driven, just without the fixture machinery. Either way, static review
   alone doesn't catch real Textual behavior (e.g. `Tree.move_cursor`
   needing a forced rebuild) — don't skip writing one because the screen
   "looks simple."
5. Wire any new keybinding into `keymap.py` and check for collisions: a key
   bound in two `BINDINGS` lists silently loses to whichever Textual resolves
   first, with no error and no visible signal beyond "the key doesn't seem to
   do anything."

## Package Layout

One module per screen under `screens/`, and per reusable widget under
`widgets/`. `app.py` owns the `App` and session lifecycle;
`screens/key_dialog.py`'s `KeyDialog`, pushed from `BrowseScreen`, owns
key-material prompts; `keymap.py` is the source of truth for keybindings;
other root-level modules (`content_preview.py`, `list_overview.py`,
`workload_grouping.py`, `repo_labels.py`, `strings.py`) are pure
presentation helpers shared across screens.
