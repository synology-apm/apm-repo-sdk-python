"""Pure unit tests for ``core/unit/select.py`` -- ``UnitModel`` ->
``NodeSpec``/``FileRow`` translation, no Textual/App/Pilot involved at all.
The disk-fs-sibling top-level-entry case mirrors
``tests/unit/browser/test_browser_unit_screen_disk_fs_nesting.py``'s
own real-provider fixtures, proven here at the pure selector level
instead of through a full Pilot run."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from synology_apm_repo.browser.content_preview import (
    render_calendar_event_preview,
    render_contact_preview,
    render_html_preview,
    render_mail_preview,
    render_teams_chat_preview,
)
from synology_apm_repo.browser.core.unit.model import FilterState, LoadedLevel, UnitModel
from synology_apm_repo.browser.core.unit.select import (
    ColumnSpec,
    FixedColumnWidth,
    FlexibleColumnWidth,
    column_headers_for,
    error_leaf_ref,
    file_table_rows,
    find_node_in_model,
    folder_tree_spec,
    is_content_only_preview,
    node_label,
    prefers_recent_content,
    preview_renderer_for,
)
from synology_apm_repo.sdk.units.base import FileState, Node, UnitKind, diagnostic_node
from synology_apm_repo.sdk.units.device_disk_fs import DISK_FS_SIBLING_REF_ATTR
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.saas.site import SITE_FLAT_CATEGORY_ATTR, SITE_LIST_OVERVIEW_ATTR


def _leaf(name: str, ref: NodeRef | None = None) -> Node:
    return Node(ref=ref or NodeRef("repo", ("root", name)), name=name, is_leaf=True)


def _container(name: str, ref: NodeRef | None = None) -> Node:
    return Node(ref=ref or NodeRef("repo", ("root", name)), name=name, is_leaf=False)


def test_node_label_escapes_rich_markup_in_the_real_name() -> None:
    """``node.name`` is real backup-derived content -- both ``Tree`` and
    ``DataTable`` re-parse a plain ``str`` label/cell as Rich markup
    (``Tree.process_label``/``DataTable``'s own ``default_cell_formatter``),
    so an unescaped name containing a bare closing tag like ``"a[/]b"``
    crashes the render with ``rich.errors.MarkupError`` instead of just
    showing oddly. ``safe()`` must have already run by the time this
    reaches either widget."""
    node = _leaf("a[/]b.txt")
    assert node_label(node) == r"a\[/]b.txt"


def test_node_label_appends_the_diagnostic_marker_for_a_diagnostic_node_placeholder() -> None:
    """A ``diagnostic_node()`` placeholder is indistinguishable from a
    real file by ``kind``/``is_leaf`` alone -- ``node_label()`` is the
    one place a caller sees a distinguishing marker, the same way it
    already does for ``FileState``."""
    node = diagnostic_node(
        NodeRef("repo", ("root", "x")), "(no filesystem recognized on this disk)", {"diagnostic": "x"}
    )
    assert node_label(node) == "(no filesystem recognized on this disk) ⚠"


def test_node_label_shows_both_the_file_state_and_diagnostic_markers_together() -> None:
    node = Node(
        ref=NodeRef("repo", ("root", "x")),
        name="x",
        is_leaf=True,
        attrs={"file_state": FileState.CLOUD_ONLY, "diagnostic": "x"},
    )
    assert node_label(node) == "x ☁ ⚠"


def test_node_label_isolates_rtl_text_in_the_real_name() -> None:
    """``node.name`` can be real RTL text (a calendar event title, a
    contact name, ...) reaching the same ``Tree``/``DataTable`` label
    the sibling escaping test above covers --
    ``synology_apm_repo.sdk.presentation.markup``'s ``safe()`` wraps it
    in a bidi isolate."""
    node = _leaf("הזמנה לאירוע")
    assert node_label(node) == "\u2068הזמנה לאירוע\u2069"


def test_node_label_omits_the_diagnostic_marker_for_an_ordinary_node() -> None:
    assert node_label(_leaf("ordinary.txt")) == "ordinary.txt"


def test_folder_tree_spec_is_none_before_the_root_has_loaded() -> None:
    assert folder_tree_spec(UnitModel()) is None


def test_folder_tree_spec_for_a_leaf_root_has_no_expand_and_no_children() -> None:
    root = _leaf("file.bin")
    spec = folder_tree_spec(UnitModel(root=root))
    assert spec is not None
    assert spec.key == root.ref
    assert spec.label == node_label(root)
    assert spec.payload is root
    assert spec.allow_expand is False
    assert spec.children is None


def test_folder_tree_spec_for_an_unloaded_container_root_is_expandable_with_no_children_yet() -> None:
    root = _container("root")
    spec = folder_tree_spec(UnitModel(root=root))
    assert spec is not None
    assert spec.allow_expand is True
    assert spec.children is None  # not yet modeled -- the screen hasn't dispatched ChildrenRequested


def test_folder_tree_spec_omits_ordinary_leaves_from_a_loaded_level() -> None:
    """An ordinary leaf never appears in the folder tree -- only in
    file_table_rows() under the same parent (see the sibling test
    below)."""
    root = _container("root")
    folder = _container("folder")
    leaf_a, leaf_b = _leaf("a"), _leaf("b")
    model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(leaf_a, folder, leaf_b), exhausted=True)})
    spec = folder_tree_spec(model)
    assert spec is not None
    assert spec.children is not None
    assert [c.key for c in spec.children] == [folder.ref]


def test_folder_tree_spec_recurses_two_levels_deep() -> None:
    root = _container("root")
    folder = _container("folder")
    subfolder = _container("subfolder")
    model = UnitModel(
        root=root,
        loaded={
            root.ref: LoadedLevel(children=(folder,), exhausted=True),
            folder.ref: LoadedLevel(children=(subfolder,), exhausted=True),
        },
    )
    spec = folder_tree_spec(model)
    assert spec is not None and spec.children is not None
    folder_spec = spec.children[0]
    assert folder_spec.allow_expand is True
    assert folder_spec.children is not None
    assert folder_spec.children[0].key == subfolder.ref


def test_filter_narrows_only_the_filtered_levels_own_children() -> None:
    root = _container("root")
    apple, banana, cherry = _container("apple"), _container("banana"), _container("cherry")
    model = UnitModel(
        root=root,
        loaded={root.ref: LoadedLevel(children=(apple, banana, cherry), exhausted=True)},
        filter=FilterState(ref=root.ref, text="an"),
    )
    spec = folder_tree_spec(model)
    assert spec is not None and spec.children is not None
    assert [c.label for c in spec.children] == ["banana"]


def test_filter_on_one_level_does_not_affect_an_unrelated_already_loaded_level() -> None:
    root = _container("root")
    folder = _container("folder")
    other_folder = _container("other")
    apple, banana = _container("apple"), _container("banana")
    model = UnitModel(
        root=root,
        loaded={
            root.ref: LoadedLevel(children=(folder, other_folder), exhausted=True),
            folder.ref: LoadedLevel(children=(apple, banana), exhausted=True),
        },
        filter=FilterState(ref=root.ref, text="folder"),  # excludes "other" at the root level only
    )
    spec = folder_tree_spec(model)
    assert spec is not None and spec.children is not None
    assert [c.label for c in spec.children] == ["folder"]

    folder_spec = spec.children[0]
    assert folder_spec.children is not None
    assert [c.label for c in folder_spec.children] == ["apple", "banana"]  # untouched by root's own filter


def _disk_image_and_fs_sibling() -> tuple[Node, Node]:
    image_ref = NodeRef("repo", ("object", "5"))
    image = Node(ref=image_ref, name="disk-1.img", is_leaf=True, kind=UnitKind.DISK_IMAGE)
    fs_ref = image_ref.child("fs")
    fs_root = Node(
        ref=fs_ref,
        name="disk-1.img (filesystem)",
        is_leaf=False,
        kind=UnitKind.DISK_FILESYSTEM,
        attrs={DISK_FS_SIBLING_REF_ATTR: image_ref},
    )
    return image, fs_root


def test_disk_fs_sibling_appears_as_an_ordinary_top_level_folder_not_nested_under_its_image() -> None:
    """The folder tree shows only containers, so the (leaf) disk-image
    node is excluded entirely and the "(filesystem)" sibling renders as
    an ordinary top-level folder-tree entry, under its own real name --
    never nested under its own disk-image node."""
    root = _container("root")
    image, fs_root = _disk_image_and_fs_sibling()
    model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(image, fs_root), exhausted=True)})
    spec = folder_tree_spec(model)
    assert spec is not None and spec.children is not None

    assert [c.key for c in spec.children] == [fs_root.ref]
    fs_spec = spec.children[0]
    assert fs_spec.payload is fs_root
    assert fs_spec.label == node_label(fs_root)  # its own real name, not a relabeled wrapper
    assert fs_spec.allow_expand is True


def test_disk_image_leaf_appears_as_an_ordinary_file_table_row() -> None:
    root = _container("root")
    image, fs_root = _disk_image_and_fs_sibling()
    model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(image, fs_root), exhausted=True)})
    rows = file_table_rows(model, root.ref)
    assert [row.node for row in rows] == [image, fs_root]


def test_list_overview_node_is_tree_visible_but_not_expandable() -> None:
    root = _container("root")
    overview = Node(
        ref=NodeRef("repo", ("root", "list")), name="MyList", is_leaf=False, attrs={SITE_LIST_OVERVIEW_ATTR: True}
    )
    model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(overview,), exhausted=True)})
    spec = folder_tree_spec(model)
    assert spec is not None and spec.children is not None
    overview_spec = spec.children[0]
    assert overview_spec.key == overview.ref  # present in the tree, unlike an ordinary leaf
    assert overview_spec.allow_expand is False
    assert overview_spec.children is None


def test_list_overview_node_yields_no_file_table_rows_of_its_own() -> None:
    root = _container("root")
    overview = Node(
        ref=NodeRef("repo", ("root", "list")), name="MyList", is_leaf=False, attrs={SITE_LIST_OVERVIEW_ATTR: True}
    )
    item = _leaf("item-1")
    model = UnitModel(
        root=root,
        loaded={
            root.ref: LoadedLevel(children=(overview,), exhausted=True),
            overview.ref: LoadedLevel(children=(item,), exhausted=True),
        },
        node_index={overview.ref: overview, item.ref: item},
    )
    assert file_table_rows(model, overview.ref) == ()


def test_a_failed_children_fetch_renders_as_a_single_synthetic_error_leaf() -> None:
    root = _container("root")
    # A bracketed substring in the error's own str() (an exception message
    # is arbitrary, provider-dependent text) must be escaped -- same
    # reasoning as node_label()'s own escaping, see its sibling test.
    model = UnitModel(root=root, errors={root.ref: "boom [/] bang"})
    spec = folder_tree_spec(model)
    assert spec is not None
    assert spec.children is not None
    assert len(spec.children) == 1
    error_spec = spec.children[0]
    assert error_spec.label == r"error: boom \[/] bang"
    assert error_spec.allow_expand is False
    assert error_spec.children is None
    assert error_spec.key == error_leaf_ref(root.ref)


def test_error_leaf_ref_never_collides_with_a_real_sibling_ref() -> None:
    ref = NodeRef("repo", ("root",))
    real_children = [_leaf(f"item-{i}", ref=ref.child(f"item-{i}")) for i in range(20)]
    assert error_leaf_ref(ref) not in {c.ref for c in real_children}


def test_a_loaded_level_takes_priority_over_a_stale_error_entry() -> None:
    """update.py's own ChildrenLoaded case clears a ref's error entry on
    success -- this proves the selector would render correctly even if
    it somehow didn't (loaded wins), rather than depending only on that
    invariant holding elsewhere."""
    root = _container("root")
    child = _container("recovered")
    model = UnitModel(
        root=root,
        loaded={root.ref: LoadedLevel(children=(child,), exhausted=True)},
        errors={root.ref: "stale error"},
    )
    spec = folder_tree_spec(model)
    assert spec is not None and spec.children is not None
    assert [c.label for c in spec.children] == ["recovered"]


def test_flat_category_node_is_tree_visible_but_not_expandable() -> None:
    """The SharePoint List *category* node gets the same tree treatment
    as an individual List's own is_list_overview group -- but for a
    different reason (its own children belong in the file table, not a
    detail-pane spreadsheet dump)."""
    root = _container("root")
    category = Node(
        ref=NodeRef("repo", ("root", "list")), name="List", is_leaf=False, attrs={SITE_FLAT_CATEGORY_ATTR: True}
    )
    model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(category,), exhausted=True)})
    spec = folder_tree_spec(model)
    assert spec is not None and spec.children is not None
    category_spec = spec.children[0]
    assert category_spec.key == category.ref  # present in the tree, unlike an ordinary leaf
    assert category_spec.allow_expand is False
    assert category_spec.children is None


def test_flat_category_nodes_own_children_are_ordinary_file_table_rows() -> None:
    """Unlike is_list_overview, is_flat_category does NOT blank the file
    table -- the category's own children (the site's individual Lists)
    are meant to be browsed there, exactly like any other folder's."""
    root = _container("root")
    category = Node(
        ref=NodeRef("repo", ("root", "list")),
        name="List",
        is_leaf=False,
        attrs={SITE_FLAT_CATEGORY_ATTR: True, "leaf_kind": UnitKind.CATEGORY_GROUP},
    )
    individual_list = Node(
        ref=NodeRef("repo", ("root", "list", "l1")),
        name="Access Requests",
        is_leaf=False,
        attrs={SITE_LIST_OVERVIEW_ATTR: True, "mtime": datetime.fromtimestamp(0, UTC)},
    )
    model = UnitModel(
        root=root,
        loaded={
            root.ref: LoadedLevel(children=(category,), exhausted=True),
            category.ref: LoadedLevel(children=(individual_list,), exhausted=True),
        },
        node_index={category.ref: category, individual_list.ref: individual_list},
    )
    rows = file_table_rows(model, category.ref)
    assert [row.node for row in rows] == [individual_list]
    assert rows[0].cells == ("Access Requests", "1970-01-01 08:00:00")


class TestColumnHeadersFor:
    def test_defaults_to_name_size_modified(self) -> None:
        root = _container("root")
        model = UnitModel(root=root)
        # The blank header between Name and Size holds the file-state
        # glyph in its own fixed-width column, kept separate so a long
        # name never shifts Size/Modified over.
        assert column_headers_for(model, root.ref) == ("Name", "", "Size", "Modified")

    def test_none_ref_defaults_too(self) -> None:
        assert column_headers_for(UnitModel(), None) == ("Name", "", "Size", "Modified")

    def test_mail_folder(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Mail", is_leaf=False, attrs={"leaf_kind": UnitKind.MAIL})
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Sender", "Subject", "Date")

    def test_contact_folder(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Contacts", is_leaf=False, attrs={"leaf_kind": UnitKind.CONTACT})
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Full Name", "Email")

    def test_calendar_folder(self) -> None:
        root = Node(
            ref=NodeRef("repo", ()), name="Calendars", is_leaf=False, attrs={"leaf_kind": UnitKind.CALENDAR_EVENT}
        )
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Title", "Start Time", "End Time", "Recurrence")

    def test_site_item_folder_defaults_like_a_file_folder(self) -> None:
        # A Document Library's own items -- SITE_ITEM shares the default
        # spec, distinguished from a List only by is_flat_category, never
        # by leaf_kind alone.
        root = Node(ref=NodeRef("repo", ()), name="Documents", is_leaf=False, attrs={"leaf_kind": UnitKind.SITE_ITEM})
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Name", "", "Size", "Modified")

    def test_category_group_kind_gets_name_created_columns(self) -> None:
        # Site's own "List" category (SITE_FLAT_CATEGORY_ATTR, read for
        # tree-expansion purposes elsewhere) declares
        # leaf_kind=CATEGORY_GROUP itself -- the column dispatch reads
        # only that, with no separate marker check.
        root = Node(
            ref=NodeRef("repo", ()),
            name="List",
            is_leaf=False,
            attrs={"leaf_kind": UnitKind.CATEGORY_GROUP, SITE_FLAT_CATEGORY_ATTR: True},
        )
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Name", "Created")

    def test_teams_chat_message_kind_gets_name_created_columns(self) -> None:
        root = Node(
            ref=NodeRef("repo", ()),
            name="Standard Channels",
            is_leaf=False,
            attrs={"leaf_kind": UnitKind.TEAMS_CHAT_MESSAGE},
        )
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Name", "Created")

    def test_raw_object_defaults_like_a_file_folder(self) -> None:
        # RawObjectProvider's own raw diagnostic listing -- not
        # TEAMS_CHAT_MESSAGE/CATEGORY_GROUP, so it stays on the default
        # spec, matching its own real Size (and no Created time).
        root = Node(ref=NodeRef("repo", ()), name="raw", is_leaf=False, attrs={"leaf_kind": UnitKind.RAW_OBJECT})
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Name", "", "Size", "Modified")

    def test_empty_folder_still_resolves_from_its_own_node_not_a_child(self) -> None:
        """The empty-folder edge case this mechanism is specifically
        designed to avoid: a folder with zero children still resolves
        correctly, since the answer comes from the folder's own node,
        never from inspecting a child."""
        root = Node(ref=NodeRef("repo", ()), name="Mail", is_leaf=False, attrs={"leaf_kind": UnitKind.MAIL})
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(), exhausted=True)})
        assert column_headers_for(model, root.ref) == ("Sender", "Subject", "Date")
        assert file_table_rows(model, root.ref) == ()


class TestColumnWidthValidation:
    def test_column_spec_rejects_a_widths_length_mismatched_with_headers(self) -> None:
        with pytest.raises(ValueError, match="widths must match headers"):
            ColumnSpec(headers=("Name", "Size"), cells=lambda node: (), widths=(FixedColumnWidth(10),))

    def test_flexible_column_width_rejects_a_zero_weight(self) -> None:
        with pytest.raises(ValueError, match="weight must be positive"):
            FlexibleColumnWidth(weight=0)

    def test_flexible_column_width_rejects_a_negative_weight(self) -> None:
        with pytest.raises(ValueError, match="weight must be positive"):
            FlexibleColumnWidth(weight=-1)


class TestPerKindCells:
    def test_mail_cells(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Mail", is_leaf=False, attrs={"leaf_kind": UnitKind.MAIL})
        mail = Node(
            ref=NodeRef("repo", ("m1",)),
            name="Hello",
            is_leaf=True,
            kind=UnitKind.MAIL,
            attrs={"sender": "Alice", "mtime": datetime.fromtimestamp(0, UTC)},
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(mail,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("Alice", "Hello", "1970-01-01 08:00:00")

    def test_mail_cells_blank_when_sender_and_date_are_unset(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Mail", is_leaf=False, attrs={"leaf_kind": UnitKind.MAIL})
        mail = Node(ref=NodeRef("repo", ("m1",)), name="Hello", is_leaf=True, kind=UnitKind.MAIL)
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(mail,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("", "Hello", "")

    def test_mail_sender_with_a_literal_bracket_is_escaped(self) -> None:
        """Regression test: ``sender`` is read straight off the backup's
        own mail row, untouched by this code -- a value shaped like a real
        tag (``"[MVP-1] Alice"``) must not reach ``DataTable`` unescaped,
        or Textual's own markup tokenizer crashes the whole file table
        with ``MarkupError`` the moment this folder is opened."""
        root = Node(ref=NodeRef("repo", ()), name="Mail", is_leaf=False, attrs={"leaf_kind": UnitKind.MAIL})
        mail = Node(
            ref=NodeRef("repo", ("m1",)),
            name="Hello",
            is_leaf=True,
            kind=UnitKind.MAIL,
            attrs={"sender": "[MVP-1] Alice"},
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(mail,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == (r"\[MVP-1] Alice", "Hello", "")

    def test_contact_cells(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Contacts", is_leaf=False, attrs={"leaf_kind": UnitKind.CONTACT})
        contact = Node(
            ref=NodeRef("repo", ("c1",)),
            name="Ada Lovelace",
            is_leaf=True,
            kind=UnitKind.CONTACT,
            attrs={"email": "ada@example.com"},
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(contact,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("Ada Lovelace", "ada@example.com")

    def test_contact_with_no_name_is_blank_not_a_raw_id(self) -> None:
        """The bug fix this whole feature grew out of: an unnamed
        contact's own Name column must never fall back to a raw
        resource-id-shaped string."""
        root = Node(ref=NodeRef("repo", ()), name="Contacts", is_leaf=False, attrs={"leaf_kind": UnitKind.CONTACT})
        contact = Node(
            ref=NodeRef("repo", ("c1",)),
            name="",  # the SDK's own _display_name() already returns "" for this case
            is_leaf=True,
            kind=UnitKind.CONTACT,
            attrs={"email": "unnamed@example.com"},
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(contact,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("", "unnamed@example.com")

    def test_calendar_event_cells(self) -> None:
        root = Node(
            ref=NodeRef("repo", ()), name="Calendars", is_leaf=False, attrs={"leaf_kind": UnitKind.CALENDAR_EVENT}
        )
        start = datetime.fromtimestamp(0, UTC)
        end = datetime.fromtimestamp(3600, UTC)
        event = Node(
            ref=NodeRef("repo", ("e1",)),
            name="Standup",
            is_leaf=True,
            kind=UnitKind.CALENDAR_EVENT,
            attrs={"event_start": start, "event_end": end, "recurrence": "Weekly"},
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(event,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("Standup", "1970-01-01 08:00:00", "1970-01-01 09:00:00", "Weekly")

    def test_calendar_event_cells_blank_when_unset(self) -> None:
        root = Node(
            ref=NodeRef("repo", ()), name="Calendars", is_leaf=False, attrs={"leaf_kind": UnitKind.CALENDAR_EVENT}
        )
        event = Node(ref=NodeRef("repo", ("e1",)), name="Standup", is_leaf=True, kind=UnitKind.CALENDAR_EVENT)
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(event,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("Standup", "", "", "")

    def test_teams_channel_cells(self) -> None:
        category = Node(
            ref=NodeRef("repo", ()),
            name="Standard Channels",
            is_leaf=False,
            attrs={"leaf_kind": UnitKind.TEAMS_CHAT_MESSAGE},
        )
        channel = Node(
            ref=NodeRef("repo", ("ch1",)),
            name="General",
            is_leaf=True,
            kind=UnitKind.TEAMS_CHAT_MESSAGE,
            attrs={"mtime": datetime.fromtimestamp(0, UTC)},
        )
        model = UnitModel(root=category, loaded={category.ref: LoadedLevel(children=(channel,), exhausted=True)})
        rows = file_table_rows(model, category.ref)
        assert rows[0].cells == ("General", "1970-01-01 08:00:00")

    def test_teams_channel_cells_blank_when_create_time_is_unavailable(self) -> None:
        # Chat's rendering is implemented generically from the format
        # docs rather than an observed instance, so create_time may be
        # absent -- a channel/chat with no create_time must still
        # render, just with a blank Created cell.
        category = Node(
            ref=NodeRef("repo", ()),
            name="Chats",
            is_leaf=False,
            attrs={"leaf_kind": UnitKind.TEAMS_CHAT_MESSAGE},
        )
        chat = Node(ref=NodeRef("repo", ("c1",)), name="Alice", is_leaf=True, kind=UnitKind.TEAMS_CHAT_MESSAGE)
        model = UnitModel(root=category, loaded={category.ref: LoadedLevel(children=(chat,), exhausted=True)})
        rows = file_table_rows(model, category.ref)
        assert rows[0].cells == ("Alice", "")


class TestFileTableRows:
    def test_none_ref_yields_no_rows(self) -> None:
        assert file_table_rows(UnitModel(), None) == ()

    def test_lists_both_files_and_subfolders(self) -> None:
        root = _container("root")
        folder = _container("folder")
        leaf = _leaf("file.txt")
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(folder, leaf), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert [row.node for row in rows] == [folder, leaf]
        # Neither folder nor leaf carries a node_leaf_kind here -- the
        # default ColumnSpec applies, whose own first column is the name.
        assert [row.cells[0] for row in rows] == [node_label(folder), node_label(leaf)]

    def test_nothing_loaded_yet_yields_no_rows(self) -> None:
        root = _container("root")
        model = UnitModel(root=root)
        assert file_table_rows(model, root.ref) == ()

    def test_a_load_failure_renders_as_a_single_synthetic_error_row(self) -> None:
        root = _container("root")
        model = UnitModel(root=root, errors={root.ref: "boom [/] bang"})
        rows = file_table_rows(model, root.ref)
        assert len(rows) == 1
        assert rows[0].node is None
        # Padded to the default ColumnSpec's own column count (Name/[file
        # state]/Size/Modified) -- the real message in the first cell,
        # every other column blank.
        assert rows[0].cells == (r"error: boom \[/] bang", "", "", "")

    def test_filter_narrows_rows_by_substring(self) -> None:
        root = _container("root")
        apple, banana = _leaf("apple"), _leaf("banana")
        model = UnitModel(
            root=root,
            loaded={root.ref: LoadedLevel(children=(apple, banana), exhausted=True)},
            filter=FilterState(ref=root.ref, text="an"),
        )
        rows = file_table_rows(model, root.ref)
        assert [row.node for row in rows] == [banana]

    def test_size_text_is_blank_for_a_node_with_no_size(self) -> None:
        root = _container("root")
        folder = _container("folder")  # containers never carry a size
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(folder,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert str(rows[0].cells[2]) == ""

    def test_size_text_formats_a_leafs_real_size(self) -> None:
        root = _container("root")
        leaf = Node(ref=NodeRef("repo", ("root", "f")), name="f", is_leaf=True, size=1024)
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(leaf,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert str(rows[0].cells[2]) == "1.0 KiB"

    def test_size_is_a_right_justified_text_value(self) -> None:
        # Textual's own DataTable.add_column has no justify parameter --
        # the Size cell itself must carry the alignment.
        root = _container("root")
        leaf = Node(ref=NodeRef("repo", ("root", "f")), name="f", is_leaf=True, size=1024)
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(leaf,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells[2].justify == "right"  # type: ignore[union-attr]

    def test_modified_text_is_blank_when_the_node_has_no_mtime(self) -> None:
        root = _container("root")
        leaf = _leaf("f")
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(leaf,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells[3] == ""

    def test_modified_text_formats_a_real_mtime(self) -> None:
        root = _container("root")
        dt = datetime.fromtimestamp(0, UTC)
        leaf = Node(ref=NodeRef("repo", ("root", "f")), name="f", is_leaf=True, attrs={"mtime": dt})
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(leaf,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells[3] == "1970-01-01 08:00:00"

    def test_file_state_cell_is_blank_for_an_ordinary_node(self) -> None:
        root = _container("root")
        leaf = _leaf("f")
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(leaf,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells[1] == ""

    def test_file_state_cell_shows_the_cloud_glyph_and_name_cell_omits_it(self) -> None:
        root = _container("root")
        leaf = Node(
            ref=NodeRef("repo", ("root", "f")),
            name="f",
            is_leaf=True,
            attrs={"file_state": FileState.CLOUD_ONLY},
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(leaf,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        # Unlike node_label() (the folder tree's own single-column label,
        # still inline), the default file-table spec's Name cell is bare
        # -- the glyph moved to its own untitled column instead.
        assert rows[0].cells[0] == "f"
        assert rows[0].cells[1] == "☁"

    def test_file_state_cell_shows_the_encrypted_glyph(self) -> None:
        root = _container("root")
        leaf = Node(
            ref=NodeRef("repo", ("root", "f")),
            name="f",
            is_leaf=True,
            attrs={"file_state": FileState.ENCRYPTED},
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(leaf,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells[1] == "🔒"

    def test_mail_folder_hides_its_own_subfolders_from_the_file_table(self) -> None:
        # A subfolder is still reachable in the folder tree -- only the
        # file table (whose columns here are Sender/Subject/Date, meaningless
        # for a subfolder) drops it.
        root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False, attrs={"leaf_kind": UnitKind.MAIL})
        subfolder = Node(
            ref=NodeRef("repo", ("root", "sub")), name="sub", is_leaf=False, attrs={"leaf_kind": UnitKind.MAIL}
        )
        mail = _leaf("hello.eml", ref=NodeRef("repo", ("root", "mail")))
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(subfolder, mail), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert [row.node for row in rows] == [mail]

    def test_calendar_category_hides_its_own_calendars_from_the_file_table(self) -> None:
        # "My Calendars"/"Other Calendars" and each individual calendar
        # are containers too, all sharing CALENDAR_EVENT as their
        # leaf_kind -- only an actual event belongs in this file table.
        category = Node(
            ref=NodeRef("repo", ("root",)),
            name="My Calendars",
            is_leaf=False,
            attrs={"leaf_kind": UnitKind.CALENDAR_EVENT},
        )
        calendar = Node(
            ref=NodeRef("repo", ("root", "cal")),
            name="台灣假日",
            is_leaf=False,
            attrs={"leaf_kind": UnitKind.CALENDAR_EVENT},
        )
        model = UnitModel(root=category, loaded={category.ref: LoadedLevel(children=(calendar,), exhausted=True)})
        assert file_table_rows(model, category.ref) == ()

    def test_default_spec_still_lists_a_document_librarys_subfolders(self) -> None:
        # leaves_only is scoped to Mail/Contact/Calendar Event only --
        # Document Library/File/Drive folders keep mixing subfolders and
        # leaves in the same file table (test_lists_both_files_and_subfolders
        # already covers the no-leaf_kind-at-all case above).
        root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False, attrs={"leaf_kind": UnitKind.SITE_ITEM})
        subfolder = Node(
            ref=NodeRef("repo", ("root", "sub")), name="sub", is_leaf=False, attrs={"leaf_kind": UnitKind.SITE_ITEM}
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(subfolder,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert [row.node for row in rows] == [subfolder]


class TestFindNodeInModel:
    def test_none_root_yields_none(self) -> None:
        assert find_node_in_model(UnitModel(), NodeRef("repo", ("x",))) is None

    def test_finds_the_root_itself(self) -> None:
        root = _container("root")
        model = UnitModel(root=root)
        assert find_node_in_model(model, root.ref) is root

    def test_finds_a_nested_child_via_the_node_index(self) -> None:
        root = _container("root")
        folder = _container("folder")
        leaf = _leaf("deep")
        model = UnitModel(
            root=root,
            loaded={
                root.ref: LoadedLevel(children=(folder,), exhausted=True),
                folder.ref: LoadedLevel(children=(leaf,), exhausted=True),
            },
            node_index={folder.ref: folder, leaf.ref: leaf},
        )
        assert find_node_in_model(model, leaf.ref) is leaf

    def test_a_ref_present_only_in_loaded_but_not_the_index_is_not_found(self) -> None:
        """Guards the O(1) lookup's own invariant: ``find_node_in_model``
        never falls back to scanning ``loaded`` -- a child missing from
        ``node_index`` (a bug in one of ``update.py``'s own fetch-result
        cases, say) must surface as "not found," not silently work anyway
        via a slower path that would mask the drift."""
        root = _container("root")
        leaf = _leaf("deep")
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(leaf,), exhausted=True)})
        assert find_node_in_model(model, leaf.ref) is None

    def test_unknown_ref_yields_none(self) -> None:
        root = _container("root")
        model = UnitModel(root=root)
        assert find_node_in_model(model, NodeRef("repo", ("nope",))) is None


class TestIsContentOnlyPreview:
    def test_mail_calendar_event_contact_and_teams_chat_message_are_content_only(self) -> None:
        for kind in (UnitKind.MAIL, UnitKind.CALENDAR_EVENT, UnitKind.CONTACT, UnitKind.TEAMS_CHAT_MESSAGE):
            node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=kind)
            assert is_content_only_preview(node)

    def test_a_raw_object_leaf_is_not_content_only(self) -> None:
        # RawObjectProvider's own raw diagnostic listing -- still wants
        # the ordinary header, unlike a Teams/Chat message's dedicated
        # UnitKind.TEAMS_CHAT_MESSAGE.
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.RAW_OBJECT)
        assert not is_content_only_preview(node)

    def test_a_file_leaf_is_not_content_only(self) -> None:
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.FILE)
        assert not is_content_only_preview(node)

    def test_a_leaf_with_no_kind_at_all_is_not_content_only(self) -> None:
        assert not is_content_only_preview(_leaf("x"))


class TestPreviewRendererFor:
    def test_mail_resolves_to_the_mail_renderer(self) -> None:
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.MAIL)
        assert preview_renderer_for(node) is render_mail_preview

    def test_calendar_event_resolves_to_the_calendar_renderer(self) -> None:
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.CALENDAR_EVENT)
        assert preview_renderer_for(node) is render_calendar_event_preview

    def test_contact_resolves_to_the_contact_renderer(self) -> None:
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.CONTACT)
        assert preview_renderer_for(node) is render_contact_preview

    def test_a_teams_chat_message_resolves_to_the_chat_renderer(self) -> None:
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.TEAMS_CHAT_MESSAGE)
        assert preview_renderer_for(node) is render_teams_chat_preview

    def test_a_plain_raw_object_leaf_falls_back_to_the_html_renderer(self) -> None:
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.RAW_OBJECT)
        assert preview_renderer_for(node) is render_html_preview

    def test_an_unlisted_kind_falls_back_to_the_html_renderer(self) -> None:
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.FILE)
        assert preview_renderer_for(node) is render_html_preview

    def test_a_leaf_with_no_kind_at_all_falls_back_to_the_html_renderer(self) -> None:
        assert preview_renderer_for(_leaf("x")) is render_html_preview


class TestPrefersRecentContent:
    def test_a_teams_chat_message_prefers_recent_content(self) -> None:
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.TEAMS_CHAT_MESSAGE)
        assert prefers_recent_content(node)

    def test_a_plain_raw_object_leaf_does_not_prefer_recent_content(self) -> None:
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.RAW_OBJECT)
        assert not prefers_recent_content(node)

    def test_mail_does_not_prefer_recent_content(self) -> None:
        # An ordinary mail/event/file's own most useful content is at
        # its start, not its end -- only a Teams/Chat message page's
        # chronological-transcript shape wants the opposite.
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.MAIL)
        assert not prefers_recent_content(node)


__all__: list[str] = []
