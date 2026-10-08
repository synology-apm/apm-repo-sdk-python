"""Unit tests for ``DirsOnlyTree`` against a real temp directory tree. Its
``".."`` leaf and type-ahead override private Textual internals
(``_populate_node``, ``_on_key``), so a Textual upgrade can break them.

Every test mounts a real ``App``: a bare ``DirectoryTree`` built outside a
running app leaks an unawaited ``watch_path`` coroutine, and
``DirectoryTree.render_label`` adds its icon prefix only once mounted, which
``get_label_width``'s comparisons depend on.
"""

from __future__ import annotations

from pathlib import Path

from textual.app import App, ComposeResult
from textual.widgets import DirectoryTree
from textual.widgets._directory_tree import DirEntry

from support.pilot import focus_widget, wait_until
from synology_apm_repo.browser.widgets.dirs_only_tree import DirsOnlyTree


class _FakeApp(App[None]):
    def __init__(self, root: Path) -> None:
        super().__init__()
        self._root = root

    def compose(self) -> ComposeResult:
        yield DirsOnlyTree(str(self._root))


# -- synthetic ".." leaf (_populate_node) ---------------------------------


async def test_root_gets_a_synthetic_dotdot_leaf_prepended_first(
    tmp_path: Path,
) -> None:
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        # Wait out the automatic initial scan (just ".." for an empty
        # tmp_path), or it races the manual _populate_node below.
        await wait_until(pilot, lambda: len(tree.root.children) == 1)
        tree._populate_node(tree.root, [tmp_path / "alpha", tmp_path / "beta"])

        labels = [str(child.label) for child in tree.root.children]
        assert labels == ["..", "alpha", "beta"]


async def test_dotdot_leaf_resolves_to_the_real_parent_directory_and_is_a_leaf(
    tmp_path: Path,
) -> None:
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 1)

        dotdot = tree.root.children[0]
        assert dotdot.data is not None
        assert dotdot.data.path == tmp_path.parent
        assert not dotdot.allow_expand  # a leaf, never itself expandable


async def test_dotdot_leaf_is_absent_at_the_filesystem_root(
    tmp_path: Path,
) -> None:
    """Rewrites the scanned root's ``data`` to a filesystem root rather than
    scanning a real one: ``_populate_node`` only compares ``Path`` objects,
    so this stays hermetic."""
    root_path = Path(Path(__file__).resolve().anchor)  # e.g. "/" (POSIX) or "C:\\" (Windows)
    assert root_path.parent == root_path
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 1)

        tree.root.data = DirEntry(root_path)
        tree._populate_node(tree.root, [])

        assert [str(child.label) for child in tree.root.children] == []


async def test_a_non_root_node_never_gets_a_dotdot_leaf(
    tmp_path: Path,
) -> None:
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 1)

        subdir_path = tmp_path / "sub"
        subdir = tree.root.add(str(subdir_path), data=DirEntry(subdir_path))
        tree._populate_node(subdir, [subdir_path / "nested"])

        assert [str(child.label) for child in subdir.children] == ["nested"]


# -- get_label_width ----------------------------------------------------


async def test_get_label_width_matches_the_base_class_for_the_dotdot_leaf(
    tmp_path: Path,
) -> None:
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        # An empty tmp_path scans to just the ".." leaf.
        await wait_until(pilot, lambda: len(tree.root.children) == 1)
        dotdot = tree.root.children[0]
        assert str(dotdot.label) == ".."

        assert DirectoryTree.get_label_width(tree, dotdot) == tree.get_label_width(dotdot)


async def test_get_label_width_matches_the_base_class_for_a_collapsed_directory(
    tmp_path: Path,
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
    tmp_path: Path,
) -> None:
    (tmp_path / "alpha").mkdir()
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 2)
        folder = next(c for c in tree.root.children if str(c.label) == "alpha")
        folder.expand()
        await wait_until(pilot, lambda: folder.is_expanded)

        assert DirectoryTree.get_label_width(tree, folder) == tree.get_label_width(folder)


# -- type-ahead jump (_on_key / _jump_to_sibling_starting_with) -----------


async def test_typeahead_jumps_to_the_first_sibling_starting_with_the_typed_letter(
    tmp_path: Path,
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
    tmp_path: Path,
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
    tmp_path: Path,
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


async def test_typeahead_ignores_non_printable_keys(
    tmp_path: Path,
) -> None:
    for name in ("alpha", "beta"):
        (tmp_path / name).mkdir()
    app = _FakeApp(tmp_path)
    async with app.run_test() as pilot:
        tree = app.query_one(DirsOnlyTree)
        await wait_until(pilot, lambda: len(tree.root.children) == 3)
        await focus_widget(pilot, tree)
        started_on = tree.cursor_node

        await pilot.press("down")

        # An arrow key falls through to Tree's own cursor movement.
        await wait_until(pilot, lambda: tree.cursor_node is not started_on)
