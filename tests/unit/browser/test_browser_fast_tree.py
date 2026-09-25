"""Unit tests for ``FastLabelTree.get_label_width`` — asserts it stays
identical to the base ``Tree.get_label_width`` it exists to avoid calling
(``Tree.render_label`` -> ``Text.assemble`` -> ``.cell_len``) rather than
just asserting some plausible-looking number, so a future edit to either
side can't silently drift the two apart. Neither ``Tree`` nor
``FastLabelTree`` needs a running ``App``/``Pilot`` for this — only
``Tree.move_cursor`` (unused here) does."""

from __future__ import annotations

from rich.text import Text
from textual.widgets import Tree

from synology_apm_repo.browser.widgets.fast_tree import FastLabelTree
from synology_apm_repo.sdk.presentation.markup import safe


def test_leaf_label_width_matches_the_base_tree() -> None:
    fast = FastLabelTree[object]("root")
    base = Tree[object]("root")
    fast_leaf = fast.root.add_leaf("a leaf label")
    base_leaf = base.root.add_leaf("a leaf label")

    assert fast.get_label_width(fast_leaf) == base.get_label_width(base_leaf)


def test_collapsed_branch_label_width_matches_the_base_tree() -> None:
    fast = FastLabelTree[object]("root")
    base = Tree[object]("root")
    fast_branch = fast.root.add("an expandable branch")
    base_branch = base.root.add("an expandable branch")

    assert not fast_branch.is_expanded
    assert fast.get_label_width(fast_branch) == base.get_label_width(base_branch)


def test_expanded_branch_label_width_matches_the_base_tree() -> None:
    fast = FastLabelTree[object]("root")
    base = Tree[object]("root")
    fast_branch = fast.root.add("an expandable branch")
    base_branch = base.root.add("an expandable branch")
    fast_branch.expand()
    base_branch.expand()

    assert fast.get_label_width(fast_branch) == base.get_label_width(base_branch)


def test_label_width_accepts_a_rich_text_label_directly() -> None:
    """``TreeNode.label`` is typed ``str | Text`` (Rich's own ``TextType``)
    — ``process_label`` always converts a ``str`` to ``Text`` before it's
    ever stored, but ``get_label_width`` still has to branch on both, per
    its own ``isinstance`` check."""
    fast = FastLabelTree[object]("root")
    base = Tree[object]("root")
    fast_leaf = fast.root.add_leaf(Text("styled label"))
    base_leaf = base.root.add_leaf(Text("styled label"))

    assert fast.get_label_width(fast_leaf) == base.get_label_width(base_leaf)


def test_label_width_scales_with_multi_cell_glyphs() -> None:
    """A guard against a byte/codepoint-count shortcut sneaking in later —
    this only stays correct if it keeps going through ``cell_len``."""
    fast = FastLabelTree[object]("root")
    base = Tree[object]("root")
    fast_leaf = fast.root.add_leaf("宽字符标签")  # wide (2-cell) CJK glyphs
    base_leaf = base.root.add_leaf("宽字符标签")

    assert fast.get_label_width(fast_leaf) == base.get_label_width(base_leaf)
    assert fast.get_label_width(fast_leaf) > len("宽字符标签")


def test_label_width_unaffected_by_bidi_isolate_marks() -> None:
    """A guard against a future Rich/Textual upgrade changing how a
    zero-width ``Cf``-category character is measured -- ``safe()``'s bidi
    isolate must stay zero-width for this to keep holding."""
    fast = FastLabelTree[object]("root")
    plain = "הזמנה לאירוע"
    wrapped = safe(plain)
    assert wrapped != plain  # sanity: the isolate was actually applied
    fast_plain_leaf = fast.root.add_leaf(plain)
    fast_wrapped_leaf = fast.root.add_leaf(wrapped)

    assert fast.get_label_width(fast_wrapped_leaf) == fast.get_label_width(fast_plain_leaf)
