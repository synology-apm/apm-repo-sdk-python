"""``DriveProvider``: OneDrive (M365) / Google Drive (GWS) via
``item_table``, built as a ``SaasWorkloadProvider`` + ``RecursiveTree``
config.

Both platforms share one schema and one provider. Content addressing
has no application-layer wrapper: ``content_object_id`` points directly
at the real file bytes, unlike Mail's META/fragment reassembly — see
``_root_folder_id`` and ``_build_tree`` for the one piece of this
provider (the synthetic root anchor) that doesn't fit ``RecursiveTree``'s
constructor unchanged.
"""

from __future__ import annotations

from ...errors import NotFoundError
from ...storage.table import Column, Table, as_int
from ..base import RestorableUnit, UnitKind, not_restorable
from .provider import (
    RecursiveTreeSaasProvider,
    SaasWorkloadConfig,
    SaasWorkloadProvider,
    make_saas_provider,
)
from .tree_strategy import RecursiveTree

_ITEM_TABLE = "item_table"
# item_table.type: 0=folder, 1=file. Real data: a folder row always has
# size=0 and an empty content_object_id.
_TYPE_FOLDER = 0

_Row = dict[str, object | None]
_Key = tuple[str, ...]


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


async def _root_folder_id(provider: SaasWorkloadProvider) -> str:
    # config_table.root_folder_id names an id with no row of its own in
    # item_table — a synthetic anchor, not a browsable item.
    table = await Table.create(provider.table(_ITEM_TABLE), "config_table", _CONFIG_COLUMNS)
    row = await table.select_one("key = ?", ("root_folder_id",))
    return str(row["value"]) if row is not None else ""


async def _build_tree(provider: SaasWorkloadProvider) -> RecursiveTree:
    return RecursiveTree(
        provider,
        table=_ITEM_TABLE,
        columns=_ITEM_COLUMNS,
        id_column="item_id",
        parent_column="parent_folder_id",
        # Looked up eagerly here since RecursiveTree's constructor takes
        # root_id directly — every other workload's "root" is just (),
        # an opaque empty key; Drive's is a real id read off a third
        # table (config_table).
        root_id=await _root_folder_id(provider),
        is_folder=lambda row: as_int(row["type"]) == _TYPE_FOLDER,
        display_name=lambda row: str(row["name"]),
        # "name" is the natural file-browser sort (required, always
        # present); tree_strategy.py's _resolve_order_by() appends
        # SQLite's own implicit ``rowid`` as the deterministic pagination
        # tiebreaker regardless.
        order_by=["name"],
    )


def _extra_attrs(provider: SaasWorkloadProvider, row: _Row) -> dict[str, object]:
    # Doesn't need ``provider`` — row-only reshaping (see
    # SaasWorkloadConfig.extra_attrs's own docstring for why the
    # signature carries a provider param at all: other workloads' extras
    # need it, this one doesn't).
    del provider
    # item_table.hash is byte-identical to the file's own meta_object_id
    # JSON's client_metadata.md5Checksum — exposed as node.attrs["hash"]
    # for a caller's own restore-integrity cross-check.
    return {"content_object_id": row["content_object_id"], "hash": row.get("hash"), "mtime": row["mtime"]}


def _item_size(row: _Row) -> int | None:
    return as_int(row["size"]) if row["size"] is not None else None


async def _assemble(provider: SaasWorkloadProvider, row: _Row, key: _Key) -> RestorableUnit:
    content_object_id = row.get("content_object_id")
    if not content_object_id:
        not_restorable("item", key)
    try:
        offset, length = await provider.object_db(_ITEM_TABLE).get(str(content_object_id))
    except NotFoundError:
        # A stale/malformed index entry (the same "recorded but not
        # actually present" shape raw_object.py's own _named_nodes
        # degrades on at listing time) — this one item just isn't
        # restorable, not a reason to crash the caller.
        not_restorable("item", key)
    view = provider.dedup_file.view(offset, length)
    size = _item_size(row)
    return RestorableUnit(
        ref=provider.ref_for(key),
        name=str(row["name"]),
        is_leaf=True,
        kind=UnitKind.DRIVE_ITEM,
        size=size,
        content=view,
    )


#: ``SaasWorkloadConfig`` behind ``DriveProvider`` — one schema shared by
#: OneDrive and Google Drive.
DRIVE_CONFIG = SaasWorkloadConfig(
    root_name="/",
    leaf_kind=UnitKind.DRIVE_ITEM,
    tables=(_ITEM_TABLE,),
    tree_factory=_build_tree,
    assemble=_assemble,
    extra_attrs=_extra_attrs,
    leaf_size=_item_size,
    # Content-level, not just metadata: "drive_db" covers GWS's own
    # Drive, M365's OneDrive (USER_DRIVE), and TEAM_DRIVE alike.
    object_names={_ITEM_TABLE: ("drive_db",)},
)


#: Constructor-style factory over ``DRIVE_CONFIG`` — see
#: ``make_saas_provider``'s own docstring for what "constructor-style
#: factory" and "no scan" mean here. ``shared`` is accepted only for
#: calling-convention uniformity with ``units/dispatch.py``'s
#: ``_ProviderFactory`` — no ``DRIVE``/``USER_DRIVE``/``TEAM_DRIVE``
#: ``sub_type`` ever offers more than this one candidate, so it is
#: always ``None`` in practice. Built as a
#: ``RecursiveTreeSaasProvider`` — Drive's flat, depth-independent
#: ``item_id`` addressing is the one SaaS shape ``units/resolve.py``
#: can't prefix-guide a descent through, so this is the one SaaS
#: factory that needs the direct-lookup capability that subclass
#: provides.
DriveProvider = make_saas_provider(DRIVE_CONFIG, name="DriveProvider", provider_cls=RecursiveTreeSaasProvider)
