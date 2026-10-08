"""Unit tests for ``core/unit/select.py``: ``UnitModel`` ->
``NodeSpec``/``FileRow`` translation, without Textual."""

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
from synology_apm_repo.sdk.units.base import FileState, ItemColumns, Node, NodeRole, UnitKind
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.provider_kit import diagnostic_node


def _leaf(name: str, ref: NodeRef | None = None) -> Node:
    return Node(ref=ref or NodeRef("repo", ("root", name)), name=name, is_leaf=True)


def _container(name: str, ref: NodeRef | None = None) -> Node:
    return Node(ref=ref or NodeRef("repo", ("root", name)), name=name, is_leaf=False)


def test_node_label_escapes_rich_markup_in_the_real_name() -> None:
    """A backup-derived name is markup-escaped: ``Tree`` and ``DataTable``
    parse a plain ``str`` as Rich markup, and a bare closing tag like
    ``"a[/]b"`` raises ``MarkupError``."""
    node = _leaf("a[/]b.txt")
    assert node_label(node) == r"a\[/]b.txt"


def test_node_label_appends_the_diagnostic_marker_for_a_diagnostic_node_placeholder() -> None:
    """``node_label()`` marks a ``diagnostic_node()`` placeholder, which
    ``kind``/``is_leaf`` can't tell apart from a real file."""
    node = diagnostic_node(NodeRef("repo", ("root", "x")), "(no filesystem recognized on this disk)", "x")
    assert node_label(node) == "(no filesystem recognized on this disk) ⚠"


def test_node_label_shows_both_the_file_state_and_diagnostic_markers_together() -> None:
    node = Node(
        ref=NodeRef("repo", ("root", "x")),
        name="x",
        is_leaf=True,
        file_state=FileState.CLOUD_ONLY,
        diagnostic="x",
    )
    assert node_label(node) == "x ☁ ⚠"


def test_node_label_isolates_rtl_text_in_the_real_name() -> None:
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
    assert spec.children is None


def test_folder_tree_spec_omits_ordinary_leaves_from_a_loaded_level() -> None:
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
    assert [c.label for c in folder_spec.children] == ["apple", "banana"]


def _disk_image_and_fs_sibling() -> tuple[Node, Node]:
    image_ref = NodeRef("repo", ("object", "5"))
    image = Node(ref=image_ref, name="disk-1.img", is_leaf=True, kind=UnitKind.DISK_IMAGE)
    fs_ref = image_ref.child("fs")
    fs_root = Node(
        ref=fs_ref,
        name="disk-1.img (filesystem)",
        is_leaf=False,
        kind=UnitKind.DISK_FILESYSTEM,
    )
    return image, fs_root


def test_disk_fs_sibling_appears_as_an_ordinary_top_level_folder_not_nested_under_its_image() -> None:
    """The ``"(filesystem)"`` sibling is a top-level tree entry under its
    own name; the disk-image leaf is not in the tree."""
    root = _container("root")
    image, fs_root = _disk_image_and_fs_sibling()
    model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(image, fs_root), exhausted=True)})
    spec = folder_tree_spec(model)
    assert spec is not None and spec.children is not None

    assert [c.key for c in spec.children] == [fs_root.ref]
    fs_spec = spec.children[0]
    assert fs_spec.payload is fs_root
    assert fs_spec.label == node_label(fs_root)
    assert fs_spec.allow_expand is True


def test_disk_image_leaf_appears_as_an_ordinary_file_table_row() -> None:
    root = _container("root")
    image, fs_root = _disk_image_and_fs_sibling()
    model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(image, fs_root), exhausted=True)})
    rows = file_table_rows(model, root.ref)
    assert [row.node for row in rows] == [image, fs_root]


def test_list_overview_node_is_tree_visible_but_not_expandable() -> None:
    root = _container("root")
    overview = Node(ref=NodeRef("repo", ("root", "list")), name="MyList", is_leaf=False, role=NodeRole.LIST_OVERVIEW)
    model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(overview,), exhausted=True)})
    spec = folder_tree_spec(model)
    assert spec is not None and spec.children is not None
    overview_spec = spec.children[0]
    assert overview_spec.key == overview.ref  # present in the tree, unlike an ordinary leaf
    assert overview_spec.allow_expand is False
    assert overview_spec.children is None


def test_list_overview_node_yields_no_file_table_rows_of_its_own() -> None:
    root = _container("root")
    overview = Node(ref=NodeRef("repo", ("root", "list")), name="MyList", is_leaf=False, role=NodeRole.LIST_OVERVIEW)
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
    # An exception message is arbitrary text, so it is markup-escaped.
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
    """A loaded level wins over an error entry for the same ref, even though
    ``update()``'s ``ChildrenLoaded`` case already clears it."""
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
    """The SharePoint List category node is in the tree but not expandable,
    like a ``NodeRole.LIST_OVERVIEW`` group; its children belong in the file
    table."""
    root = _container("root")
    category = Node(ref=NodeRef("repo", ("root", "list")), name="List", is_leaf=False, role=NodeRole.FLAT_CATEGORY)
    model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(category,), exhausted=True)})
    spec = folder_tree_spec(model)
    assert spec is not None and spec.children is not None
    category_spec = spec.children[0]
    assert category_spec.key == category.ref  # present in the tree, unlike an ordinary leaf
    assert category_spec.allow_expand is False
    assert category_spec.children is None


def test_flat_category_nodes_own_children_are_ordinary_file_table_rows() -> None:
    """Unlike ``NodeRole.LIST_OVERVIEW``, ``NodeRole.FLAT_CATEGORY`` keeps the file table:
    the category's children (the site's Lists) are browsed there."""
    root = _container("root")
    category = Node(
        ref=NodeRef("repo", ("root", "list")),
        name="List",
        is_leaf=False,
        role=NodeRole.FLAT_CATEGORY,
        leaf_kind=UnitKind.CATEGORY_GROUP,
    )
    individual_list = Node(
        ref=NodeRef("repo", ("root", "list", "l1")),
        name="Access Requests",
        is_leaf=False,
        role=NodeRole.LIST_OVERVIEW,
        mtime=datetime.fromtimestamp(0, UTC),
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
        # The blank header is the file-state glyph's own fixed-width column.
        assert column_headers_for(model, root.ref) == ("Name", "", "Size", "Modified")

    def test_none_ref_defaults_too(self) -> None:
        assert column_headers_for(UnitModel(), None) == ("Name", "", "Size", "Modified")

    def test_mail_folder(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Mail", is_leaf=False, leaf_kind=UnitKind.MAIL)
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Sender", "Subject", "Date")

    def test_contact_folder(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Contacts", is_leaf=False, leaf_kind=UnitKind.CONTACT)
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Full Name", "Email")

    def test_calendar_folder(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Calendars", is_leaf=False, leaf_kind=UnitKind.CALENDAR_EVENT)
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Title", "Start Time", "End Time", "Recurrence")

    def test_site_item_folder_defaults_like_a_file_folder(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Documents", is_leaf=False, leaf_kind=UnitKind.SITE_ITEM)
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Name", "", "Size", "Modified")

    def test_category_group_kind_gets_name_created_columns(self) -> None:
        # The column dispatch reads only leaf_kind, not the role.
        root = Node(
            ref=NodeRef("repo", ()),
            name="List",
            is_leaf=False,
            leaf_kind=UnitKind.CATEGORY_GROUP,
            role=NodeRole.FLAT_CATEGORY,
        )
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Name", "Created")

    def test_teams_chat_message_kind_gets_name_created_columns(self) -> None:
        root = Node(
            ref=NodeRef("repo", ()),
            name="Standard Channels",
            is_leaf=False,
            leaf_kind=UnitKind.TEAMS_CHAT_MESSAGE,
        )
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Name", "Created")

    def test_raw_object_defaults_like_a_file_folder(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="raw", is_leaf=False, leaf_kind=UnitKind.RAW_OBJECT)
        model = UnitModel(root=root)
        assert column_headers_for(model, root.ref) == ("Name", "", "Size", "Modified")

    def test_empty_folder_still_resolves_from_its_own_node_not_a_child(self) -> None:
        """An empty folder still gets its kind's headers: they come from the
        folder's own ``leaf_kind``, not from a child."""
        root = Node(ref=NodeRef("repo", ()), name="Mail", is_leaf=False, leaf_kind=UnitKind.MAIL)
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
        root = Node(ref=NodeRef("repo", ()), name="Mail", is_leaf=False, leaf_kind=UnitKind.MAIL)
        mail = Node(
            ref=NodeRef("repo", ("m1",)),
            name="Hello",
            is_leaf=True,
            kind=UnitKind.MAIL,
            mtime=datetime.fromtimestamp(0, UTC),
            columns=ItemColumns(sender="Alice"),
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(mail,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("Alice", "Hello", "1970-01-01 08:00:00")

    def test_mail_cells_blank_when_sender_and_date_are_unset(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Mail", is_leaf=False, leaf_kind=UnitKind.MAIL)
        mail = Node(ref=NodeRef("repo", ("m1",)), name="Hello", is_leaf=True, kind=UnitKind.MAIL)
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(mail,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("", "Hello", "")

    def test_mail_sender_with_a_literal_bracket_is_escaped(self) -> None:
        """A backup-derived ``sender`` like ``"[MVP-1] Alice"`` is
        markup-escaped, or ``DataTable`` raises ``MarkupError``."""
        root = Node(ref=NodeRef("repo", ()), name="Mail", is_leaf=False, leaf_kind=UnitKind.MAIL)
        mail = Node(
            ref=NodeRef("repo", ("m1",)),
            name="Hello",
            is_leaf=True,
            kind=UnitKind.MAIL,
            columns=ItemColumns(sender="[MVP-1] Alice"),
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(mail,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == (r"\[MVP-1] Alice", "Hello", "")

    def test_contact_cells(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Contacts", is_leaf=False, leaf_kind=UnitKind.CONTACT)
        contact = Node(
            ref=NodeRef("repo", ("c1",)),
            name="Ada Lovelace",
            is_leaf=True,
            kind=UnitKind.CONTACT,
            columns=ItemColumns(email="ada@example.com"),
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(contact,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("Ada Lovelace", "ada@example.com")

    def test_contact_with_no_name_is_blank_not_a_raw_id(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Contacts", is_leaf=False, leaf_kind=UnitKind.CONTACT)
        contact = Node(
            ref=NodeRef("repo", ("c1",)),
            name="",
            is_leaf=True,
            kind=UnitKind.CONTACT,
            columns=ItemColumns(email="unnamed@example.com"),
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(contact,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("", "unnamed@example.com")

    def test_calendar_event_cells(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Calendars", is_leaf=False, leaf_kind=UnitKind.CALENDAR_EVENT)
        start = datetime.fromtimestamp(0, UTC)
        end = datetime.fromtimestamp(3600, UTC)
        event = Node(
            ref=NodeRef("repo", ("e1",)),
            name="Standup",
            is_leaf=True,
            kind=UnitKind.CALENDAR_EVENT,
            columns=ItemColumns(event_start=start, event_end=end, recurrence="Weekly"),
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(event,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("Standup", "1970-01-01 08:00:00", "1970-01-01 09:00:00", "Weekly")

    def test_calendar_event_cells_blank_when_unset(self) -> None:
        root = Node(ref=NodeRef("repo", ()), name="Calendars", is_leaf=False, leaf_kind=UnitKind.CALENDAR_EVENT)
        event = Node(ref=NodeRef("repo", ("e1",)), name="Standup", is_leaf=True, kind=UnitKind.CALENDAR_EVENT)
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(event,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells == ("Standup", "", "", "")

    def test_teams_channel_cells(self) -> None:
        category = Node(
            ref=NodeRef("repo", ()),
            name="Standard Channels",
            is_leaf=False,
            leaf_kind=UnitKind.TEAMS_CHAT_MESSAGE,
        )
        channel = Node(
            ref=NodeRef("repo", ("ch1",)),
            name="General",
            is_leaf=True,
            kind=UnitKind.TEAMS_CHAT_MESSAGE,
            mtime=datetime.fromtimestamp(0, UTC),
        )
        model = UnitModel(root=category, loaded={category.ref: LoadedLevel(children=(channel,), exhausted=True)})
        rows = file_table_rows(model, category.ref)
        assert rows[0].cells == ("General", "1970-01-01 08:00:00")

    def test_teams_channel_cells_blank_when_create_time_is_unavailable(self) -> None:
        # A chat may lack create_time; its Created cell is then blank.
        category = Node(
            ref=NodeRef("repo", ()),
            name="Chats",
            is_leaf=False,
            leaf_kind=UnitKind.TEAMS_CHAT_MESSAGE,
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
        # Padded to the default spec's column count.
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
        folder = _container("folder")
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
        # DataTable.add_column has no justify parameter; the cell carries it.
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
        leaf = Node(ref=NodeRef("repo", ("root", "f")), name="f", is_leaf=True, mtime=dt)
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
            file_state=FileState.CLOUD_ONLY,
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(leaf,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        # Unlike node_label(), the Name cell carries no inline glyph.
        assert rows[0].cells[0] == "f"
        assert rows[0].cells[1] == "☁"

    def test_file_state_cell_shows_the_encrypted_glyph(self) -> None:
        root = _container("root")
        leaf = Node(
            ref=NodeRef("repo", ("root", "f")),
            name="f",
            is_leaf=True,
            file_state=FileState.ENCRYPTED,
        )
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(leaf,), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert rows[0].cells[1] == "🔒"

    def test_mail_folder_hides_its_own_subfolders_from_the_file_table(self) -> None:
        root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False, leaf_kind=UnitKind.MAIL)
        subfolder = Node(ref=NodeRef("repo", ("root", "sub")), name="sub", is_leaf=False, leaf_kind=UnitKind.MAIL)
        mail = _leaf("hello.eml", ref=NodeRef("repo", ("root", "mail")))
        model = UnitModel(root=root, loaded={root.ref: LoadedLevel(children=(subfolder, mail), exhausted=True)})
        rows = file_table_rows(model, root.ref)
        assert [row.node for row in rows] == [mail]
        spec = folder_tree_spec(model)
        assert spec is not None and spec.children is not None
        assert [c.key for c in spec.children] == [subfolder.ref]

    def test_calendar_category_hides_its_own_calendars_from_the_file_table(self) -> None:
        # A CALENDAR_EVENT folder (leaves_only) drops container children.
        category = Node(
            ref=NodeRef("repo", ("root",)),
            name="My Calendars",
            is_leaf=False,
            leaf_kind=UnitKind.CALENDAR_EVENT,
        )
        calendar = Node(
            ref=NodeRef("repo", ("root", "cal")),
            name="台灣假日",
            is_leaf=False,
            leaf_kind=UnitKind.CALENDAR_EVENT,
        )
        model = UnitModel(root=category, loaded={category.ref: LoadedLevel(children=(calendar,), exhausted=True)})
        assert file_table_rows(model, category.ref) == ()

    def test_default_spec_still_lists_a_document_librarys_subfolders(self) -> None:
        # leaves_only applies to Mail/Contact/Calendar Event only; other
        # folders list subfolders in the file table too.
        root = Node(ref=NodeRef("repo", ("root",)), name="root", is_leaf=False, leaf_kind=UnitKind.SITE_ITEM)
        subfolder = Node(ref=NodeRef("repo", ("root", "sub")), name="sub", is_leaf=False, leaf_kind=UnitKind.SITE_ITEM)
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
        """``find_node_in_model`` reads only ``node_index``, never scanning
        ``loaded``."""
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
        node = Node(ref=NodeRef("repo", ("root", "x")), name="x", is_leaf=True, kind=UnitKind.MAIL)
        assert not prefers_recent_content(node)
