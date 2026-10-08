"""``open_contact_provider``: M365/GWS Contact via ``contact_table``, built as
a ``SaasWorkloadProvider`` + ``SyntheticGroupedTree`` config. An empty
``contact_table`` is a valid, empty listing.

- **M365**: contacts are grouped by ``parent_folder_id``, named via
  ``contact_folder_table``. The META JSON's ``client_metadata`` holds
  Graph API contact fields; each contact exports as CSV
  (``build_contact_csv``).
- **GWS**: contacts sit in one "Contacts" group; their many-to-many group
  memberships (``_gws_contact_groups``) become a ``groups`` detail. The META
  JSON (People API fields, plus ``photo_*`` keys when there's a photo)
  exports as-is.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping

from ...storage.table import Column
from ..base import ItemColumns, UnitKind
from ..content.saas_artifact import LazyArtifact
from ..content.saas_contact import build_contact_csv
from .provider import NodeExtras, SaasWorkloadConfig, SaasWorkloadProvider, make_saas_provider
from .tree_strategy import Key, Row, SyntheticGroupedTree
from .workload_helpers import group_display_name_resolver, membership_detail

_CONTACT_TABLE = "contact_table"
_CONTACT_FOLDER_TABLE = "contact_folder_table"  # M365 only, lives in contact_folder_db
# GWS only: definitions (group_id, group_name) in contact_group_db;
# membership (contact_id, group_id) in contact_db.
_GROUP_DEFINITION_TABLE = "group_table"
_GROUP_MEMBERSHIP_TABLE = "contact_group_table"
_ALL_CONTACTS_GROUP = "Contacts"

_CONTACT_FOLDER_DB_NAMES = ("contact_folder_db",)
_GROUP_DEFINITION_DB_NAMES = ("contact_group_db",)
_CONTACT_DB_NAMES_FOR_GROUP_MEMBERSHIP = ("contact_db",)


@dataclasses.dataclass(frozen=True, slots=True)
class ContactState:
    """What the contact ``tree_factory`` prefetches for ``_contact_extras``.

    Attributes:
        gws_groups: GWS contact group names, by ``contact_id``; empty for M365.
    """

    gws_groups: Mapping[str, list[str]] = dataclasses.field(default_factory=dict)


_Provider = SaasWorkloadProvider[ContactState]


_CONTACT_COLUMNS = [
    Column("contact_id"),
    Column("first_name"),
    Column("last_name"),
    Column("meta_object_id"),
    Column("parent_folder_id", required=False),  # M365 only
    Column("primary_email", required=False),
]


def _display_name(row: Row) -> str:
    """A contact's full name, or ``""`` when neither ``first_name`` nor
    ``last_name`` is set (never the opaque ``contact_id``)."""
    parts = [str(row["first_name"] or ""), str(row["last_name"] or "")]
    return " ".join(p for p in parts if p)


async def _m365_contact_folder_names(provider: _Provider) -> dict[str, str] | None:
    """Best-effort ``folder_id -> display name`` map from M365's
    ``contact_folder_table``; ``None`` if unavailable, and groups then
    show the raw folder id."""
    return await provider.read_id_to_name_map(
        _CONTACT_FOLDER_DB_NAMES, _CONTACT_FOLDER_TABLE, id_column="folder_id", name_column="folder_name"
    )


async def _gws_contact_groups(provider: _Provider) -> dict[str, list[str]] | None:
    """Best-effort ``contact_id -> [group names]`` map for GWS."""
    return await provider.read_grouped_names(
        definition_names=_GROUP_DEFINITION_DB_NAMES,
        definition_table=_GROUP_DEFINITION_TABLE,
        id_column="group_id",
        name_column="group_name",
        membership_names=_CONTACT_DB_NAMES_FOR_GROUP_MEMBERSHIP,
        membership_table=_GROUP_MEMBERSHIP_TABLE,
        item_column="contact_id",
        group_column="group_id",
    )


def _contact_extras(provider: _Provider, row: Row) -> NodeExtras:
    email = row.get("primary_email")
    return NodeExtras(
        columns=ItemColumns(email=str(email) if email else None),
        details=membership_detail(provider.state.gws_groups, row["contact_id"], "groups"),
    )


async def _build_tree(provider: _Provider) -> tuple[SyntheticGroupedTree, ContactState]:
    is_m365 = provider.is_m365
    folder_names: dict[str, str] | None = None
    groups: dict[str, list[str]] = {}
    if is_m365:
        folder_names = await _m365_contact_folder_names(provider)
    else:
        groups = await _gws_contact_groups(provider) or {}

    tree = SyntheticGroupedTree(
        provider,
        table=_CONTACT_TABLE,
        columns=_CONTACT_COLUMNS,
        id_column="contact_id",
        # None (GWS): every contact in the one _ALL_CONTACTS_GROUP.
        group_column="parent_folder_id" if is_m365 else None,
        display_name=_display_name,
        root_name=_ALL_CONTACTS_GROUP,
        # The M365 Portal's own order.
        order_by=["first_name", "last_name"],
        group_display_name=group_display_name_resolver(folder_names),
    )
    return tree, ContactState(gws_groups=groups)


def _export_name(provider: _Provider, row: Row) -> str:
    # A file name can't be empty, so an unnamed contact uses its id here only.
    name = _display_name(row) or str(row["contact_id"])
    return f"{name}.csv" if provider.is_m365 else f"{name}.json"


async def _content(provider: _Provider, row: Row, key: Key) -> LazyArtifact:
    is_m365 = provider.is_m365
    meta_object_id = str(row["meta_object_id"])

    async def _build() -> bytes:
        meta_bytes = await provider.read_object(_CONTACT_TABLE, meta_object_id)
        return build_contact_csv(meta_bytes) if is_m365 else meta_bytes

    return LazyArtifact(_build)


#: ``SaasWorkloadConfig`` behind ``open_contact_provider``.
CONTACT_CONFIG = SaasWorkloadConfig(
    root_name=_ALL_CONTACTS_GROUP,
    leaf_kind=UnitKind.CONTACT,
    tables=(_CONTACT_TABLE,),
    tree_factory=_build_tree,
    content=_content,
    leaf_export_name=_export_name,
    leaf_extras=_contact_extras,
    # M365 and GWS alike. A GROUP_EXCHANGE version has no contact_db, so
    # the candidate is simply not found there.
    object_names={_CONTACT_TABLE: ("contact_db",)},
)


#: Async factory over ``CONTACT_CONFIG`` (see ``make_saas_provider``).
open_contact_provider = make_saas_provider(CONTACT_CONFIG, name="open_contact_provider")
