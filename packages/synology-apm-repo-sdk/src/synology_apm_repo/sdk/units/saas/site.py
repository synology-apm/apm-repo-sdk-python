"""``SiteProvider``: M365 SharePoint Site (including document
libraries) via ``list_version_table`` + ``item_version_table``, built as
a ``SaasWorkloadProvider`` + ``NamedGroupRecursiveTree`` config, wrapped
in one extra synthetic level (``tree_strategy.CategorizedGroupTree``)
that splits the root into "Document Library" and "List" groups, keyed as
``(category, *inner_key)``. The "List" category node is additionally
marked ``SITE_FLAT_CATEGORY_ATTR`` (see below).

M365-only — SharePoint has no GWS equivalent (FORMAT-SPEC.md: sharepoint-site).
``item_version_table`` has 19 real columns; ``_ITEM_COLUMNS`` below
reads only the subset this module needs.

**Two List shapes, one rule**: a "general list" (a data row + optional
attachments) and a "document library" (a file or folder) never need a
separate lookup to tell apart for content purposes — content addressing
always goes through the META object's own ``content_list[].object_id``,
never ``item_version_table.file_object_id`` (a write-path punch-hole
shortcut only, FORMAT-SPEC.md: sharepoint-site). A row with no
attachment (a plain list row, or a document-library folder) has an empty
``content_list``; its own ``values`` become its content instead.

**Tree**: List → Item, grouped by ``list_id``; nested document-library
folders additionally use parent-pointer recursion via
``parent_folder_id`` within one list — ``NamedGroupRecursiveTree``.
``item_type`` encodes SharePoint's own ``FileSystemObjectType`` (numeric
File=0/Folder=1, or the string equivalents "FILE"/"FOLDER").

**A document library's real top level is not always the empty
string** — ``list_version_table.root_folder_id`` is each list's own
real top-level anchor (general lists use ``""``; document libraries use
their own non-empty id). ``_build_tree`` reads this per list and
passes it to ``NamedGroupRecursiveTree`` as ``root_folder_id_of`` —
without it, a document library's real (non-empty) top-level anchor
would never match the tree's default empty-string root, and the
library would appear empty.
"""

from __future__ import annotations

from typing import cast

from ...errors import NotFoundError
from ...storage.table import Column, Table, as_int, as_str
from ..base import ContentSource, Node, RestorableUnit, UnitKind, mtime_attrs, not_restorable
from ..content.saas_artifact import LazyArtifact, parse_meta_json
from ..content.saas_site import build_values_json
from .objectdb import read_object
from .provider import SaasWorkloadConfig, SaasWorkloadProvider, make_saas_provider
from .tree_strategy import CategorizedGroupTree, FolderPredicate, NamedGroupRecursiveTree

_LIST_TABLE = "list_version_table"
_ITEM_TABLE = "item_version_table"

_Row = dict[str, object | None]
_Key = tuple[str, ...]

#: The two synthetic top-level groups the browser presents (see
#: ``tree_strategy.CategorizedGroupTree``). Internal key tokens, not the
#: displayed strings — ``_CATEGORY_LABELS`` maps these to what the user
#: actually sees, same separation as ``browse_screen.py``'s own
#: ``_TYPE_LABELS``.
_CATEGORY_DOC_LIBRARY = "document_library"
_CATEGORY_LIST = "list"
_CATEGORY_LABELS = {_CATEGORY_DOC_LIBRARY: "Document Library", _CATEGORY_LIST: "List"}

#: ``required=False`` on ``list_type``/``root_folder_id``: absent from the
#: synthetic unit-test DB (``tests/unit/sdk/test_units_saas_site.py``) —
#: ``Table``'s own schema-drift tolerance backfills ``None`` for those rows,
#: treated as "not a document library" by ``_is_document_library`` and
#: "no real per-list anchor" (falls back to the empty-string default) by
#: ``_build_tree`` below.
_LIST_COLUMNS = [
    Column("list_id"),
    Column("list_title"),
    Column("meta_object_id"),
    Column("list_type", required=False),
    Column("root_folder_id", required=False),
    # Real, byte-for-byte equal to its own META JSON's
    # ``metadata.Created`` -- used as the list-level group node's own
    # "Created"/Modified mtime, so a document library's top-level row
    # isn't blank in that column.
    Column("create_time", required=False),
]
_ITEM_COLUMNS = [
    Column("item_id"),
    Column("list_id"),
    Column("file_id"),
    Column("parent_folder_id"),
    Column("title"),
    Column("item_type"),
    Column("meta_object_id"),
    # ``url_path`` (FORMAT-SPEC.md: sharepoint-site) names display in a browsing UI as
    # one of its two documented uses (the other — reconstructing a
    # restore-target path — isn't this project's concern). Needed
    # because a document-library *folder* row's own ``title`` is empty —
    # ``_display_name`` falls back to this.
    Column("url_path"),
    # ``value1`` caches a document-library file's own real byte size for
    # display (FORMAT-SPEC.md: sharepoint-site) — ``_leaf_size`` reads it so a Site
    # file gets the same listing-time ``Node.size`` a Drive item already
    # does, instead of always ``None``.
    Column("value1", required=False),
    # A document-library item's own real modified time — equal to its
    # own META JSON's ``values["Modified"]`` — read the same way
    # Drive/FS already populate ``Node.attrs["mtime"]`` (see
    # ``_item_extra_attrs``).
    Column("mtime", required=False),
]


def _self_id_of(row: _Row) -> str:
    file_id = str(row["file_id"])
    return file_id if file_id else str(row["item_id"])


def _is_folder(row: _Row) -> bool:
    # item_type is FileSystemObjectType for doc-lib items ("FILE"/"FOLDER"
    # or their numeric equivalents) but means something else for general
    # list rows (FORMAT-SPEC.md: sharepoint-site) — general list rows are never
    # folders, so an item is only ever treated as a folder when this
    # column unambiguously says so. The numeric encoding is SharePoint's
    # own FileSystemObjectType (File=0, Folder=1). Compared as
    # ``str(...)`` on both sides rather than a raw ``int`` membership check:
    # SQLite's own
    # type affinity can store/return a TEXT-affinity column's numeric
    # value as either an ``int`` or a ``str`` depending on how it was
    # written, and ``str(1) == "1"`` regardless of which one this
    # particular connector version's row actually is.
    #
    # _FOLDER_SQL below must keep classifying the same rows as this
    # function -- callers must keep the two forms agreeing, since
    # there's no way to derive one from the other generically.
    return str(row["item_type"]) in ("FOLDER", "1")


#: The SQL form of ``_is_folder`` -- see ``FolderPredicate``.
_FOLDER_SQL = "CAST(item_type AS TEXT) IN ('FOLDER', '1')"

#: The SQL form of ``_display_name``'s full branching, in the same
#: priority order, including its ``_self_id_of`` fallback (``file_id``
#: when set, else ``item_id``) -- must stay in sync with ``_display_name``
#: for the same reason ``_FOLDER_SQL`` must stay in sync with
#: ``_is_folder``: there's no way to derive one form from the other
#: generically, so both must be hand-kept in agreement. Sorts by the
#: *full* ``url_path`` rather than extracting its basename: every row this
#: expression orders is a sibling under the same parent (the query
#: already scopes to one), so the shared path prefix contributes nothing
#: to the comparison and sorting by the full path is equivalent to
#: sorting by the basename alone.
_NAME_ORDER_SQL = """
CASE
    WHEN file_id IS NOT NULL AND file_id != '' AND url_path IS NOT NULL AND RTRIM(url_path, '/') != '' THEN url_path
    WHEN title IS NOT NULL AND title != '' THEN title
    WHEN file_id IS NOT NULL AND file_id != '' THEN file_id
    ELSE item_id
END
""".strip()


def _display_name(row: _Row) -> str:
    # A document-library item's own ``title`` is a SharePoint *content*
    # title, not its real file name (e.g. ``title="Home"`` for the file a
    # file browser shows as "Home.aspx") — ``url_path`` carries the real
    # file name instead. A folder's own ``title`` is simply empty, so the
    # same url_path-derived name applies there too. ``file_id`` — non-empty
    # only for a document-library item (FORMAT-SPEC.md: sharepoint-site; the same
    # signal ``_self_id_of`` already keys on) — is what tells this case
    # apart from a general List row, whose own ``title`` is exactly what
    # should show and is never a file-path artifact.
    if row.get("file_id"):
        url_path = str(row.get("url_path") or "").rstrip("/")
        if url_path:
            return url_path.rsplit("/", 1)[-1]
    title = str(row["title"])
    if title:
        return title
    return _self_id_of(row)


def _leaf_size(row: _Row) -> int | None:
    # ``value1`` is only ever a real byte size for a document-library item
    # (``file_id`` non-empty — the same per-row signal ``_display_name``
    # uses). For a general List row, ``value1`` means something else
    # entirely per row (a plain list row: "null"; some rows: a small
    # unrelated cached integer) — gating on ``file_id`` avoids ever
    # misreporting one of those as a size. A folder also has a
    # non-empty ``file_id`` but its own ``value1``
    # is "null" — ``int()`` raising is what correctly leaves it sizeless
    # too (folders never reach this function regardless — ``leaf_size``
    # only applies to leaves — but the guard costs nothing to keep).
    if not row.get("file_id"):
        return None
    try:
        return int(str(row.get("value1") or ""))
    except ValueError:
        return None


def _item_extra_attrs(provider: SaasWorkloadProvider, row: _Row) -> dict[str, object]:
    # Row-only reshaping — doesn't need ``provider``, but the shared
    # extra_attrs callback signature always takes one, since other
    # workloads' extras (GWS Mail's label names, GWS Contact's group
    # names) read prefetched data from ``provider.extras``.
    del provider
    return mtime_attrs(row.get("mtime"))


def _is_document_library(list_row: _Row) -> bool:
    # list_type=1 marks a document library (Documents, Form Templates,
    # Site Pages, Style Library, Master Page Gallery, ...); list_type=0
    # marks a general list (Access Requests, Events,
    # TaxonomyHiddenList, User Information List, ...) — see
    # FORMAT-SPEC.md: sharepoint-site. None (missing column, or a genuinely unset
    # value) is treated as "not a document library" rather than
    # raising, matching ``_LIST_COLUMNS``' own ``required=False`` tolerance
    # for this column.
    return list_row.get("list_type") == 1


async def _build_tree(provider: SaasWorkloadProvider) -> CategorizedGroupTree:
    # One direct scan of list_version_table computes every one of this
    # workload's list->category mappings, each list's own creation time,
    # *and* each list's own top-level anchor (root_folder_id_by_list --
    # a document library's real top-level anchor is a non-empty per-list
    # id, not the shared empty-string default a general list uses;
    # without resolving it per list, NamedGroupRecursiveTree's WHERE
    # clause never matches those top-level rows and the library appears
    # empty), up front — small and bounded (a site's
    # own list count, not its item count). Done before constructing
    # ``inner`` below since its own constructor needs
    # root_folder_id_by_list already built. Not read via
    # ``inner.children_of(())``/``row_for()``: that pair only ever exposes
    # ``list_id``/``list_title`` (children_of) or a *leaf item*'s own row
    # (row_for returns None for any key shorter than 2 segments — a
    # group's key is always 1) — neither carries ``list_type``/
    # ``root_folder_id``/``create_time``. Also feeds provider.extras below
    # (group_attrs), rather than issuing a second, separate scan for that.
    list_table = await Table.create(provider.table(_LIST_TABLE), _LIST_TABLE, _LIST_COLUMNS)
    list_categories: dict[str, str] = {}
    list_create_time: dict[str, int] = {}
    root_folder_id_by_list: dict[str, str] = {}
    async for row in list_table.select():
        list_id = str(row["list_id"])
        list_categories[list_id] = _CATEGORY_DOC_LIBRARY if _is_document_library(row) else _CATEGORY_LIST
        root_folder_id_by_list[list_id] = str(row.get("root_folder_id") or "")
        create_time = row.get("create_time")
        if create_time is not None:
            list_create_time[list_id] = as_int(create_time)
    provider.extras["site_list_create_time"] = list_create_time

    # No I/O of its own beyond the scan above — ``async`` only because
    # ``SaasWorkloadConfig``'s one ``tree_factory`` field type has to cover
    # Drive's, which does read.
    inner = NamedGroupRecursiveTree(
        provider,
        group_table=_LIST_TABLE,
        group_columns=_LIST_COLUMNS,
        group_id_column="list_id",
        group_name_column="list_title",
        leaf_table=_ITEM_TABLE,
        leaf_columns=_ITEM_COLUMNS,
        self_id_of=_self_id_of,
        leaf_group_column="list_id",
        leaf_parent_column="parent_folder_id",
        folder=FolderPredicate(is_folder=_is_folder, sql=_FOLDER_SQL),
        display_name=_display_name,
        # _NAME_ORDER_SQL, not a plain "title" order_by: a document-library
        # item's displayed name comes from url_path, not title (see
        # _display_name), so the sort key must branch the same way to
        # keep sort order matching display order. item_version_table has
        # no real index on (list_id, parent_folder_id) in the real schema
        # (FORMAT-SPEC.md: sharepoint-site) — apply_index_hint() builds one
        # into this table's own private per-version temp copy the first
        # time this tree is used (see NamedGroupRecursiveTree's own
        # construction).
        order_by_sql=_NAME_ORDER_SQL,
        root_folder_id_of=lambda list_id: root_folder_id_by_list.get(list_id, ""),
    )
    return CategorizedGroupTree(inner, categories=list_categories, labels=_CATEGORY_LABELS)


#: Marks exactly a plain List's own group node (never the root, a
#: category, or a document-library group) — the browser's
#: ``unit_screen.py`` reads this via ``is_list_overview`` to (a) render
#: the node as a non-expandable tree leaf (this List's own items are
#: never individually tree-navigable — a spreadsheet-style overview of
#: them is enough) and (b) build that overview by reading the items
#: directly through the provider instead. A named constant (and
#: ``is_list_overview`` below), not a bare string literal read
#: independently at each of the browser's own call sites, so a typo on
#: either side is a real ``NameError``/``ImportError`` instead of a
#: silent "always False."
SITE_LIST_OVERVIEW_ATTR = "site_list_overview"

#: Marks exactly the site root's own "List" category node (never an
#: individual List, a document-library group, or the root itself) — the
#: browser reads this via ``is_flat_category`` to hide every individual
#: List from the folder tree (no recursion into this node's own children
#: for tree purposes) while still listing them as ordinary file-table
#: rows, same as any other folder's children. Unlike
#: ``SITE_LIST_OVERVIEW_ATTR``, this node stays perfectly ordinary for
#: selection/navigation purposes — only the tree-expansion decision
#: changes.
SITE_FLAT_CATEGORY_ATTR = "site_flat_category"


def is_list_overview(node: Node) -> bool:
    """Whether ``node`` is a plain List's own group node — see
    ``SITE_LIST_OVERVIEW_ATTR``."""
    return bool(node.attrs.get(SITE_LIST_OVERVIEW_ATTR))


def is_flat_category(node: Node) -> bool:
    """Whether ``node`` is the site root's own "List" category node —
    see ``SITE_FLAT_CATEGORY_ATTR``."""
    return bool(node.attrs.get(SITE_FLAT_CATEGORY_ATTR))


def _group_attrs(provider: SaasWorkloadProvider, key: _Key) -> dict[str, object]:
    if key == (_CATEGORY_LIST,):
        return {SITE_FLAT_CATEGORY_ATTR: True, "leaf_kind": UnitKind.CATEGORY_GROUP}
    if len(key) != 2:
        # A key this long is a folder nested *within* a document
        # library (CategorizedGroupTree's own (category, *inner_key) --
        # exactly 2 segments is a list/library's own top-level group;
        # anything longer is one of its own subfolders, which carries no
        # list-level Created time of its own).
        return {}
    category, list_id = key
    attrs: dict[str, object] = {SITE_LIST_OVERVIEW_ATTR: True} if category == _CATEGORY_LIST else {}
    # A real create_time exists for both categories -- a document
    # library's own top-level group node gets the same "Created" mtime
    # a List's does, so its own file-table row isn't blank in the
    # Modified column.
    create_time = cast("dict[str, int]", provider.extras.get("site_list_create_time", {})).get(list_id)
    attrs.update(mtime_attrs(create_time))
    return attrs


async def _assemble(provider: SaasWorkloadProvider, row: _Row, key: _Key) -> RestorableUnit:
    meta_object_id = row.get("meta_object_id")
    if meta_object_id is None:
        not_restorable("item", key)

    # The META object is small (a JSON document, not the file content
    # itself) — reading it eagerly here to decide which ContentSource
    # shape to hand back is the same cost class as FsProvider eagerly
    # opening its shared dedup.img in unit(); it is not the (possibly
    # large) file content itself.
    meta_bytes = await read_object(provider.object_db(_ITEM_TABLE), provider.dedup_file, str(meta_object_id))
    meta = parse_meta_json(meta_bytes, f"site item {meta_object_id!r} META", ref=str(meta_object_id))
    content_list = meta.get("content_list") or []
    if content_list:
        # FORMAT-SPEC.md: sharepoint-site: the content address is the META's own
        # content_list[].object_id, never
        # item_version_table.file_object_id (a punch-hole shortcut).
        object_id = as_str(content_list[0]["object_id"])
        try:
            content_offset, content_length = await provider.object_db(_ITEM_TABLE).get(object_id)
        except NotFoundError:
            # A stale/malformed index entry (the same "recorded but
            # not actually present" shape raw_object.py's own
            # _named_nodes degrades on at listing time) — this one item
            # just isn't restorable, not a reason to crash the caller.
            not_restorable("item", key)
        content: ContentSource = provider.dedup_file.view(content_offset, content_length)
    else:
        # No attached file (a plain list row, or a doc-lib folder with
        # no FILE_ content entry) — the item's own field values are
        # its content.
        values_bytes = build_values_json(meta.get("values") or {})

        async def _values() -> bytes:
            # Already in memory (assembled above) — this coroutine only
            # exists to satisfy LazyArtifact's awaitable build hook.
            return values_bytes

        content = LazyArtifact(_values)

    return RestorableUnit(
        ref=provider.ref_for(key), name=_display_name(row), is_leaf=True, kind=UnitKind.SITE_ITEM, content=content
    )


#: ``SaasWorkloadConfig`` behind ``SiteProvider`` — Lists/document
#: libraries via ``tree_strategy.CategorizedGroupTree``.
SITE_CONFIG = SaasWorkloadConfig(
    root_name="Lists",
    leaf_kind=UnitKind.SITE_ITEM,
    tables=(_LIST_TABLE, _ITEM_TABLE),
    tree_factory=_build_tree,
    assemble=_assemble,
    # Site is M365-only, no GWS equivalent naming to reconcile (see
    # units/saas/object_name_index.py).
    object_names={_LIST_TABLE: ("site_list_db",), _ITEM_TABLE: ("site_item_db",)},
    group_attrs=_group_attrs,
    extra_attrs=_item_extra_attrs,
    leaf_size=_leaf_size,
)


#: Constructor-style factory over ``SITE_CONFIG`` — callable exactly
#: like a constructor (``await SiteProvider(repo, version, saas_streams)``),
#: resolving the service DB via the connector's own object-name index
#: only, never a scan. ``shared`` is accepted only for
#: calling-convention uniformity
#: with ``units/dispatch.py``'s ``_ProviderFactory`` — ``SITE`` never
#: offers more than this one candidate, so it is always ``None`` in
#: practice.
SiteProvider = make_saas_provider(SITE_CONFIG, name="SiteProvider")
