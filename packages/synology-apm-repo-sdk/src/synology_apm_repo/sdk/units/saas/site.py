"""``open_site_provider``: M365 SharePoint Site (FORMAT-SPEC.md: SharePoint Site) via
``list_version_table`` + ``item_version_table``. M365-only.

- Tree: "Document Library"/"List" categories (``CategorizedGroupTree``)
  → list → items, with document-library folders nested by
  ``parent_folder_id`` (``NamedGroupRecursiveTree``). Each list's
  top-level anchor is its ``root_folder_id`` (``""`` for a general list,
  a non-empty id for a document library).
- Content: an item with a ``content_list`` entry (an attached file) reads
  that object; one without (a plain list row, a folder) exports its
  ``values`` as JSON.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping

from ..._util.jsonparse import parse_json_object
from ...concurrency import bounded_gather
from ...storage.table import Column, Table, as_int, as_str
from ...units.provider_kit import mtime_from_raw, not_restorable
from ..base import ContentSource, Node, NodeRole, UnitKind, UnitProvider
from ..content.saas_artifact import LazyArtifact
from ..content.saas_site import build_values_json
from .provider import NodeExtras, SaasWorkloadConfig, SaasWorkloadProvider, make_saas_provider
from .tree_strategy import CategorizedGroupTree, FolderPredicate, Key, NamedGroupRecursiveTree, Row

_LIST_TABLE = "list_version_table"
_ITEM_TABLE = "item_version_table"


@dataclasses.dataclass(frozen=True, slots=True)
class SiteState:
    """What ``_build_tree`` prefetches for ``_group_extras``.

    Attributes:
        list_create_time: Each list's ``create_time``, by ``list_id``.
    """

    list_create_time: Mapping[str, int]


_Provider = SaasWorkloadProvider[SiteState]


#: Key tokens of the two categories; ``_CATEGORY_LABELS`` holds their
#: displayed names.
_CATEGORY_DOC_LIBRARY = "document_library"
_CATEGORY_LIST = "list"
_CATEGORY_LABELS = {_CATEGORY_DOC_LIBRARY: "Document Library", _CATEGORY_LIST: "List"}

#: A missing ``list_type`` means a general list; a missing
#: ``root_folder_id`` means the ``""`` anchor.
_LIST_COLUMNS = [
    Column("list_id"),
    Column("list_title"),
    Column("meta_object_id"),
    Column("list_type", required=False),
    Column("root_folder_id", required=False),
    # The list group node's mtime (equals META metadata.Created).
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
    Column("url_path"),  # a document-library item's file name; see _display_name
    Column("value1", required=False),  # a document-library file's size; see _leaf_size
    Column("mtime", required=False),
]


def _self_id_of(row: Row) -> str:
    file_id = str(row["file_id"])
    return file_id or str(row["item_id"])


def _is_folder(row: Row) -> bool:
    # item_type is SharePoint's FileSystemObjectType for a document-library
    # item ("FOLDER" or 1; the value may come back as int or str) and means
    # something else for a general list row, which is never a folder.
    # Keep in sync with _FOLDER_SQL.
    return str(row["item_type"]) in ("FOLDER", "1")


#: The SQL form of ``_is_folder`` -- see ``FolderPredicate``.
_FOLDER_SQL = "CAST(item_type AS TEXT) IN ('FOLDER', '1')"

#: The SQL form of ``_display_name``, for sorting; keep the two in sync.
#: The full ``url_path`` sorts like its basename, since siblings share the
#: prefix.
_NAME_ORDER_SQL = """
CASE
    WHEN file_id IS NOT NULL AND file_id != '' AND url_path IS NOT NULL AND RTRIM(url_path, '/') != '' THEN url_path
    WHEN title IS NOT NULL AND title != '' THEN title
    WHEN file_id IS NOT NULL AND file_id != '' THEN file_id
    ELSE item_id
END
""".strip()


def _display_name(row: Row) -> str:
    # A document-library item (non-empty file_id) is named by its
    # url_path basename: its title is a content title ("Home" for
    # "Home.aspx"), empty for a folder. A general list row uses its title.
    if row.get("file_id"):
        url_path = str(row.get("url_path") or "").rstrip("/")
        if url_path:
            return url_path.rsplit("/", 1)[-1]
    title = str(row["title"])
    if title:
        return title
    return _self_id_of(row)


def _leaf_size(row: Row) -> int | None:
    # value1 is a byte size only for a document-library item (non-empty
    # file_id); on a general list row it means something else.
    if not row.get("file_id"):
        return None
    try:
        return int(str(row.get("value1") or ""))
    except ValueError:
        return None


def _item_extras(provider: _Provider, row: Row) -> NodeExtras:
    del provider
    return NodeExtras(mtime=mtime_from_raw(row.get("mtime")))


def _is_document_library(list_row: Row) -> bool:
    # list_type 1: a document library (Documents, Site Pages, ...); 0 or
    # None: a general list.
    return list_row.get("list_type") == 1


async def _build_tree(provider: _Provider) -> tuple[CategorizedGroupTree, SiteState]:
    # Each list's category, anchor and create time, up front; bounded by
    # the list count.
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
        # Sorts by the displayed name, not title.
        order_by_sql=_NAME_ORDER_SQL,
        root_folder_id_of=lambda list_id: root_folder_id_by_list.get(list_id, ""),
    )
    tree = CategorizedGroupTree(inner, categories=list_categories, labels=_CATEGORY_LABELS)
    return tree, SiteState(list_create_time=list_create_time)


def _group_extras(provider: _Provider, key: Key) -> NodeExtras:
    if key == (_CATEGORY_LIST,):
        return NodeExtras(role=NodeRole.FLAT_CATEGORY, leaf_kind=UnitKind.CATEGORY_GROUP)
    if len(key) != 2:
        # Only (category, list_id) is a list's own group node.
        return NodeExtras()
    category, list_id = key
    return NodeExtras(
        mtime=mtime_from_raw(provider.state.list_create_time.get(list_id)),
        role=NodeRole.LIST_OVERVIEW if category == _CATEGORY_LIST else NodeRole.ORDINARY,
    )


async def _content(provider: _Provider, row: Row, key: Key) -> ContentSource:
    meta_object_id = row.get("meta_object_id")
    if meta_object_id is None:
        not_restorable("item", key)

    # The small META object is read eagerly to choose the content shape.
    meta_bytes = await provider.read_object(_ITEM_TABLE, str(meta_object_id))
    meta = parse_json_object(meta_bytes, f"site item {meta_object_id!r} META", ref=str(meta_object_id))
    content_list = meta.get("content_list") or []
    if content_list:
        # The content address is content_list[].object_id, never
        # item_version_table.file_object_id (FORMAT-SPEC.md: SharePoint Site).
        return await provider.object_view(_ITEM_TABLE, as_str(content_list[0]["object_id"]), key)
    values_bytes = build_values_json(meta.get("values") or {})

    async def _values() -> bytes:
        return values_bytes

    return LazyArtifact(_values)


#: ``SaasWorkloadConfig`` behind ``open_site_provider`` — Lists/document
#: libraries via ``tree_strategy.CategorizedGroupTree``.
SITE_CONFIG = SaasWorkloadConfig(
    root_name="Lists",
    leaf_kind=UnitKind.SITE_ITEM,
    tables=(_LIST_TABLE, _ITEM_TABLE),
    tree_factory=_build_tree,
    content=_content,
    object_names={_LIST_TABLE: ("site_list_db",), _ITEM_TABLE: ("site_item_db",)},
    group_extras=_group_extras,
    leaf_extras=_item_extras,
    leaf_size=_leaf_size,
)


#: Async factory over ``SITE_CONFIG`` (see ``make_saas_provider``).
open_site_provider = make_saas_provider(SITE_CONFIG, name="open_site_provider")


@dataclasses.dataclass(frozen=True, slots=True)
class SiteListItems:
    """A SharePoint List's items, each as its field dict.

    Attributes:
        rows: One field dict per readable item, in the order
            ``provider.children()`` listed them.
        truncated: Whether ``item_cap`` items were listed, so the List
            may hold more than ``rows``.
    """

    rows: list[dict[str, object]]
    truncated: bool


async def read_site_list_items(
    provider: UnitProvider, node: Node, *, item_cap: int, read_limit: int, max_concurrent: int
) -> SiteListItems:
    """Up to ``item_cap`` of the items of ``node`` (a List group node,
    ``NodeRole.LIST_OVERVIEW``), each read through at most ``read_limit``
    bytes, ``max_concurrent`` at a time, for showing the List as one table.
    Best-effort past the listing: an item that can't be opened, read or
    parsed as a JSON object is left out rather than failing the whole List.

    Raises:
        ApmRepoError: Listing the List's items failed.
    """
    children = await provider.children(node, offset=0, limit=item_cap)
    # A plain List has no nested folders; skip any defensively.
    leaves = [child for child in children if child.is_leaf]
    rows_by_index: list[dict[str, object] | None] = [None] * len(leaves)

    async def _load_one(item: tuple[int, Node]) -> None:
        index, child = item
        try:
            unit = await provider.unit(child)
            values = json.loads(await unit.content.read(0, read_limit))
        except Exception:  # noqa: BLE001 - one malformed item must never fail the whole List
            return
        if isinstance(values, dict):
            rows_by_index[index] = values

    await bounded_gather(enumerate(leaves), _load_one, max_concurrent=max_concurrent)
    return SiteListItems(rows=[row for row in rows_by_index if row is not None], truncated=len(children) == item_cap)
