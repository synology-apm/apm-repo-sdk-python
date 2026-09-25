"""Unit tests for ``DirsOnlyTree`` — driven directly, against a real temp
directory tree, independent of ``ConnectDialog`` (whose own Pilot tests,
``test_browser_pilot_connect_dialog.py``/``test_browser_pilot_remote_browser.py``,
only ever exercise this widget incidentally through its own path-picker
UI, never its ``".."``-leaf/type-ahead/``get_label_width`` mechanics
directly). This is the single stalest file in the browser package:
``DirsOnlyTree``'s ``".."`` up-navigation leaf and type-ahead both reach
into private Textual internals that could break on a Textual upgrade —
worth a dedicated, focused test for exactly that reason.

Every test here mounts a real ``App``/``Pilot`` — unlike
``test_browser_fast_tree.py``'s own bare, unmounted ``Tree`` construction,
constructing a bare ``DirectoryTree`` (this class's own base) outside a
running app leaks an unawaited ``watch_path`` coroutine (a real Textual
quirk: its ``path`` reactive's watcher is ``async def``, scheduled the
moment ``__init__`` sets it, with no running event loop yet to pick it
up). ``get_label_width`` needs a mounted tree for an unrelated reason
too: ``DirectoryTree.render_label`` only adds its real icon prefix once
``is_mounted`` is true, so an unmounted comparison would silently compare
against the wrong baseline.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from textual.app import App, ComposeResult
from textual.widgets import DirectoryTree
from textual.widgets._directory_tree import DirEntry

from synology_apm_repo.browser.widgets.dirs_only_tree import DirsOnlyTree


class _FakeApp(App[None]):
    def __init__(self, root: Path) -> None:
        super().__init__()
        self._root = root

    def compose(self) -> ComposeResult:
        yield DirsOnlyTree(str(self._root))


# -- synthetic ".." leaf (_populate_node) ---------------------------------


async def test_root_gets_a_synthetic_dotdot_leaf_prepended_first(tmp_path: Path, wait_until: Any) -> None:
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        # Waits out the widget's own automatic initial scan (tmp_path is
        # empty, so it settles at just the ".." leaf) before manually
        # re-populating below -- otherwise the two race, and whichever
        # runs last silently wins.
        await wait_until(pilot, lambda: len(tree.root.children) == 1)
        tree._populate_node(tree.root, [tmp_path / "alpha", tmp_path / "beta"])

        labels = [str(child.label) for child in tree.root.children]
        assert labels == ["..", "alpha", "beta"]


async def test_dotdot_leaf_resolves_to_the_real_parent_directory_and_is_a_leaf(tmp_path: Path, wait_until: Any) -> None:
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 1)

        dotdot = tree.root.children[0]
        assert dotdot.data is not None
        assert dotdot.data.path == tmp_path.parent
        assert not dotdot.allow_expand  # a leaf, never itself expandable


async def test_dotdot_leaf_is_absent_at_the_filesystem_root(tmp_path: Path, wait_until: Any) -> None:
    """Simulates "no parent to go up to" by rewriting the (already
    mounted, already-scanned) root's own ``data`` in place to a path
    that is its own parent (a filesystem root, e.g. ``/``), rather than
    pointing the whole widget's automatic scanner at a real filesystem
    root -- ``_populate_node`` only ever compares ``Path`` objects here
    (no filesystem I/O), so this stays hermetic and avoids a second race
    against that scanner re-firing mid-test."""
    root_path = Path(Path(__file__).resolve().anchor)  # e.g. "/" (POSIX) or "C:\\" (Windows)
    assert root_path.parent == root_path  # test's own assumption about Path semantics
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 1)

        tree.root.data = DirEntry(root_path)
        tree._populate_node(tree.root, [])

        assert [str(child.label) for child in tree.root.children] == []


async def test_a_non_root_node_never_gets_a_dotdot_leaf(tmp_path: Path, wait_until: Any) -> None:
    """Only the tree's own root ever gets the synthetic ``".."`` leaf --
    a deeper node's already-collapsed parent needs no separate way to
    hide its children."""
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 1)

        subdir_path = tmp_path / "sub"
        subdir = tree.root.add(str(subdir_path), data=DirEntry(subdir_path))
        tree._populate_node(subdir, [subdir_path / "nested"])

        assert [str(child.label) for child in subdir.children] == ["nested"]


# -- get_label_width -- needs a real, mounted tree ------------------------


async def test_get_label_width_matches_the_base_class_for_the_dotdot_leaf(tmp_path: Path, wait_until: Any) -> None:
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        # tmp_path has no real subdirectories of its own here, so the real
        # async scan (never manually driven -- avoids racing this test's
        # own _populate_node call against it) yields just the ".." leaf.
        await wait_until(pilot, lambda: len(tree.root.children) == 1)
        dotdot = tree.root.children[0]
        assert str(dotdot.label) == ".."

        assert DirectoryTree.get_label_width(tree, dotdot) == tree.get_label_width(dotdot)


async def test_get_label_width_matches_the_base_class_for_a_collapsed_directory(
    tmp_path: Path, wait_until: Any
) -> None:
    (tmp_path / "alpha").mkdir()
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 2)
        folder = next(c for c in tree.root.children if str(c.label) == "alpha")
        assert not folder.is_expanded

        assert DirectoryTree.get_label_width(tree, folder) == tree.get_label_width(folder)


async def test_get_label_width_matches_the_base_class_for_an_expanded_directory(
    tmp_path: Path, wait_until: Any
) -> None:
    (tmp_path / "alpha").mkdir()
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 2)
        folder = next(c for c in tree.root.children if str(c.label) == "alpha")
        folder.expand()
        await pilot.pause()

        assert DirectoryTree.get_label_width(tree, folder) == tree.get_label_width(folder)


# -- type-ahead jump (_on_key / _jump_to_sibling_starting_with) -----------


async def test_typeahead_jumps_to_the_first_sibling_starting_with_the_typed_letter(
    tmp_path: Path, wait_until: Any, focus_widget: Any
) -> None:
    for name in ("alpha", "beta", "gamma"):
        (tmp_path / name).mkdir()
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 4)
        await focus_widget(pilot, tree)

        await pilot.press("b")
        await wait_until(pilot, lambda: str(tree.cursor_node.label) == "beta" if tree.cursor_node else False)


async def test_typeahead_extends_the_buffer_across_keystrokes(
    tmp_path: Path, wait_until: Any, focus_widget: Any
) -> None:
    for name in ("alpha", "aardvark"):
        (tmp_path / name).mkdir()
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 3)
        await focus_widget(pilot, tree)

        await pilot.press("a")
        await wait_until(pilot, lambda: str(tree.cursor_node.label) == "aardvark" if tree.cursor_node else False)
        await pilot.press("l")
        await wait_until(pilot, lambda: str(tree.cursor_node.label) == "alpha" if tree.cursor_node else False)


async def test_typeahead_restarts_from_the_last_keystroke_on_no_match(
    tmp_path: Path, wait_until: Any, focus_widget: Any
) -> None:
    for name in ("alpha", "beta"):
        (tmp_path / name).mkdir()
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 3)
        await focus_widget(pilot, tree)

        await pilot.press("a")
        await wait_until(pilot, lambda: str(tree.cursor_node.label) == "alpha" if tree.cursor_node else False)
        # "ab" matches nothing; falls back to "b" alone, matching "beta".
        await pilot.press("b")
        await wait_until(pilot, lambda: str(tree.cursor_node.label) == "beta" if tree.cursor_node else False)


async def test_typeahead_ignores_non_printable_keys(tmp_path: Path, wait_until: Any, focus_widget: Any) -> None:
    for name in ("alpha", "beta"):
        (tmp_path / name).mkdir()
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 3)
        await focus_widget(pilot, tree)
        started_on = tree.cursor_node

        await pilot.press("down")
        await pilot.pause()

        # An arrow key must fall through to Tree's own cursor movement,
        # never feed the type-ahead buffer -- the cursor should have moved
        # by exactly Tree's own normal amount, not jumped to a letter match.
        assert tree.cursor_node is not started_on
