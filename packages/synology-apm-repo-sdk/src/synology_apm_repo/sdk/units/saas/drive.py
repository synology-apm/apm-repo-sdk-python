"""``open_drive_provider``: OneDrive (M365) / Google Drive (GWS) via
``item_table``, built as a ``SaasWorkloadProvider`` + ``RecursiveTree``
config.

Both platforms share one schema and one provider. ``content_object_id``
points directly at the file's bytes, with no application-layer wrapper.
"""

from __future__ import annotations

from ...storage.table import Column, Table, as_int
from ...units.provider_kit import mtime_from_raw, not_restorable
from ..base import ContentSource, UnitKind
from .provider import (
    NodeExtras,
    RecursiveTreeSaasProvider,
    SaasWorkloadConfig,
    SaasWorkloadProvider,
    make_saas_provider,
)
from .tree_strategy import FolderPredicate, Key, RecursiveTree, Row

_ITEM_TABLE = "item_table"
# item_table.type: 0=folder, 1=file. A folder row has size=0 and an
# empty content_object_id.
_TYPE_FOLDER = 0


_Provider = SaasWorkloadProvider[None]


_ITEM_COLUMNS = [
    Column("item_id"),
    Column("name"),
    Column("size"),
    Column("mtime"),
    Column("meta_object_id"),
    Column("content_object_id"),
    Column("type"),
    Column("parent_folder_id"),
    Column("hash", required=False),
    Column("etag", required=False),
    Column("starred", required=False),
]

_CONFIG_COLUMNS = [Column("key"), Column("value")]


async def _root_folder_id(provider: _Provider) -> str:
    # config_table.root_folder_id names an id with no row of its own in
    # item_table — a synthetic anchor, not a browsable item.
    table = await Table.create(provider.table(_ITEM_TABLE), "config_table", _CONFIG_COLUMNS)
    row = await table.select_one("key = ?", ("root_folder_id",))
    return str(row["value"]) if row is not None else ""


async def _build_tree(provider: _Provider) -> tuple[RecursiveTree, None]:
    tree = RecursiveTree(
        provider,
        table=_ITEM_TABLE,
        columns=_ITEM_COLUMNS,
        id_column="item_id",
        parent_column="parent_folder_id",
        root_id=await _root_folder_id(provider),
        folder=FolderPredicate(is_folder=lambda row: as_int(row["type"]) == _TYPE_FOLDER, sql=f"type = {_TYPE_FOLDER}"),
        display_name=lambda row: str(row["name"]),
        order_by=["name"],
    )
    return tree, None


def _extras(provider: _Provider, row: Row) -> NodeExtras:
    del provider
    # item_table.hash equals the META object's client_metadata.md5Checksum,
    # exposed for a caller's restore-integrity check.
    return NodeExtras(
        mtime=mtime_from_raw(row["mtime"]),
        details={"content_object_id": row["content_object_id"], "hash": row.get("hash")},
    )


def _item_size(row: Row) -> int | None:
    return as_int(row["size"]) if row["size"] is not None else None


async def _content(provider: _Provider, row: Row, key: Key) -> ContentSource:
    content_object_id = row.get("content_object_id")
    if not content_object_id:
        not_restorable("item", key)
    return await provider.object_view(_ITEM_TABLE, str(content_object_id), key)


#: ``SaasWorkloadConfig`` behind ``open_drive_provider`` — one schema shared by
#: OneDrive and Google Drive.
DRIVE_CONFIG = SaasWorkloadConfig(
    root_name="/",
    leaf_kind=UnitKind.DRIVE_ITEM,
    tables=(_ITEM_TABLE,),
    tree_factory=_build_tree,
    content=_content,
    leaf_extras=_extras,
    leaf_size=_item_size,
    # "drive_db" covers GWS Drive, M365 OneDrive (USER_DRIVE) and TEAM_DRIVE.
    object_names={_ITEM_TABLE: ("drive_db",)},
)


#: Async factory over ``DRIVE_CONFIG`` (see ``make_saas_provider``),
#: building a ``RecursiveTreeSaasProvider`` for Drive's depth-independent
#: ``item_id`` refs.
open_drive_provider = make_saas_provider(
    DRIVE_CONFIG, name="open_drive_provider", provider_cls=RecursiveTreeSaasProvider
)
