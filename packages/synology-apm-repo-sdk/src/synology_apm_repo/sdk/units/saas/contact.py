"""``ContactProvider``: M365/GWS Contact via ``contact_table``,
built as a ``SaasWorkloadProvider`` + ``SyntheticGroupedTree`` config.

An empty ``contact_table`` (a tenant with zero contacts) is a legitimate
state, not a gap — ``ContactProvider`` still constructs successfully and
finds a real, empty table.

**The two platforms differ enough that they aren't one shared code
path**:

- **M365** (``contact_table`` has ``parent_folder_id`` — a real,
  single-parent folder hierarchy resolved to names via
  ``_m365_contact_folder_names``): metadata JSON is
  ``{"version":"1.0","client_metadata":{...Graph API contact fields,
  camelCase...},"contact_type":"Contact"}``; no photo concept at all.
  Exported as CSV — the only portable format M365 Contact has (no
  vCard, no PST-equivalent); see ``build_contact_csv``.
- **GWS** (``contact_table`` has no folder column — contacts instead
  belong to zero or more *groups*, an M:N relationship, not a
  hierarchy): metadata JSON is
  ``{"version":"2.0","client_metadata":{...People API Person
  fields...},"photo_object_id":...}`` (the last three keys —
  ``photo_size``/``photo_hash``/``photo_object_id`` — only present when
  the contact has a photo). Surfaced as raw JSON. Group membership
  is a real many-to-many relationship, not synthesized — see
  ``_gws_contact_groups`` — including Google's own built-in
  ``"myContacts"`` system group alongside any user-named ones.

Tree: a synthetic top-level grouping — M365's real, name-resolved
``parent_folder_id``, a single synthetic "Contacts" bucket for GWS —
then a flat leaf per contact (the "grouped flat list" strategy,
``SyntheticGroupedTree``).
"""

from __future__ import annotations

from ...storage.table import Column
from ..base import RestorableUnit, UnitKind
from ..content.saas_artifact import LazyArtifact
from ..content.saas_contact import build_contact_csv
from .object_name_index import read_grouped_names, read_id_to_name_map
from .objectdb import read_object
from .provider import (
    SaasWorkloadConfig,
    SaasWorkloadProvider,
    extras_attr,
    group_display_name_resolver,
    make_saas_provider,
)
from .tree_strategy import SyntheticGroupedTree

_CONTACT_TABLE = "contact_table"
_CONTACT_FOLDER_TABLE = "contact_folder_table"  # M365 only, lives in contact_folder_db
# GWS only. Unlike mail.py's own label tables, these two are uniquely
# named (no schema-sniffing ambiguity risk) — group_table (definitions:
# group_id, group_name) lives in contact_group_db; contact_group_table
# (membership: contact_id, group_id) lives inside contact_db itself,
# alongside contact_table.
_GROUP_DEFINITION_TABLE = "group_table"
_GROUP_MEMBERSHIP_TABLE = "contact_group_table"
_ALL_CONTACTS_GROUP = "Contacts"

_CONTACT_FOLDER_DB_NAMES = ("contact_folder_db",)
_GROUP_DEFINITION_DB_NAMES = ("contact_group_db",)
_CONTACT_DB_NAMES_FOR_GROUP_MEMBERSHIP = ("contact_db",)

_Row = dict[str, object | None]
_Key = tuple[str, ...]

_CONTACT_COLUMNS = [
    Column("contact_id"),
    Column("first_name"),
    Column("last_name"),
    Column("meta_object_id"),
    Column("parent_folder_id", required=False),  # M365 only
]


def _display_name(row: _Row) -> str:
    parts = [str(row["first_name"] or ""), str(row["last_name"] or "")]
    name = " ".join(p for p in parts if p)
    return name or str(row["contact_id"])


async def _m365_contact_folder_names(provider: SaasWorkloadProvider) -> dict[str, str] | None:
    """Best-effort ``folder_id -> display name`` map from M365's own
    ``contact_folder_table`` (same object-name-index mechanism as
    ``mail.py``'s own ``_m365_folder_names()``; columns ``row_id,
    folder_id, folder_name, parent_folder_id``). Returns ``None`` on any
    failure, in which case the caller falls back to the raw
    ``parent_folder_id`` value — costs a label, never correctness."""
    return await read_id_to_name_map(
        provider.dedup_file,
        provider.object_name_index,
        _CONTACT_FOLDER_DB_NAMES,
        _CONTACT_FOLDER_TABLE,
        id_column="folder_id",
        name_column="folder_name",
    )


async def _gws_contact_groups(provider: SaasWorkloadProvider) -> dict[str, list[str]] | None:
    """Best-effort ``contact_id -> [real group names]`` map from GWS's
    own group mechanism — an M:N relationship, so this becomes an
    ``extra_attrs`` field (``CONTACT_CONFIG``) rather than a tree
    grouping, same reasoning as ``mail.py``'s own GWS labels. Resolved
    via the object-name index only, same as ``_m365_contact_folder_names``."""
    return await read_grouped_names(
        provider.dedup_file,
        provider.object_name_index,
        definition_names=_GROUP_DEFINITION_DB_NAMES,
        definition_table=_GROUP_DEFINITION_TABLE,
        id_column="group_id",
        name_column="group_name",
        membership_names=_CONTACT_DB_NAMES_FOR_GROUP_MEMBERSHIP,
        membership_table=_GROUP_MEMBERSHIP_TABLE,
        item_column="contact_id",
        group_column="group_id",
    )


def _contact_extra_attrs(provider: SaasWorkloadProvider, row: _Row) -> dict[str, object]:
    return extras_attr(provider, "gws_contact_groups", row["contact_id"], "groups")


async def _build_tree(provider: SaasWorkloadProvider) -> SyntheticGroupedTree:
    is_m365 = provider.is_m365
    folder_names: dict[str, str] | None = None
    if is_m365:
        folder_names = await _m365_contact_folder_names(provider)
    else:
        provider.extras["gws_contact_groups"] = await _gws_contact_groups(provider) or {}

    return SyntheticGroupedTree(
        provider,
        table=_CONTACT_TABLE,
        columns=_CONTACT_COLUMNS,
        id_column="contact_id",
        # GWS Contact groups are a genuine M:N relationship (a contact
        # can belong to more than one — see this module's own docstring),
        # so there is no single-parent column to filter by: group_column=
        # None means every contact sits in the one synthetic
        # _ALL_CONTACTS_GROUP, queried without any WHERE at all.
        group_column="parent_folder_id" if is_m365 else None,
        display_name=_display_name,
        root_name=_ALL_CONTACTS_GROUP,
        # M365's own Portal contact list is itself ``ORDER BY first_name``
        # (FORMAT-SPEC.md: m365-contact) — last_name/rowid are the tiebreakers
        # (both required, always present; see tree_strategy.py's own
        # _resolve_order_by()).
        order_by=["first_name", "last_name"],
        group_display_name=group_display_name_resolver(folder_names),
    )


async def _assemble(provider: SaasWorkloadProvider, row: _Row, key: _Key) -> RestorableUnit:
    # No I/O here — the real reads happen inside the LazyArtifact's own
    # (awaited-at-most-once) build callback.
    is_m365 = provider.is_m365
    meta_object_id = str(row["meta_object_id"])

    async def _build() -> bytes:
        meta_bytes = await read_object(provider.object_db(_CONTACT_TABLE), provider.dedup_file, meta_object_id)
        return build_contact_csv(meta_bytes) if is_m365 else meta_bytes

    name = _display_name(row)
    suffix = "csv" if is_m365 else "json"
    return RestorableUnit(
        ref=provider.ref_for(key),
        name=f"{name}.{suffix}",
        is_leaf=True,
        kind=UnitKind.CONTACT,
        content=LazyArtifact(_build),
    )


#: ``SaasWorkloadConfig`` behind ``ContactProvider`` — see this module's
#: own docstring for the M365/GWS split.
CONTACT_CONFIG = SaasWorkloadConfig(
    root_name=_ALL_CONTACTS_GROUP,
    leaf_kind=UnitKind.CONTACT,
    tables=(_CONTACT_TABLE,),
    tree_factory=_build_tree,
    assemble=_assemble,
    extra_attrs=_contact_extra_attrs,
    # Both M365 and GWS use "contact_db" for this table (see
    # units/saas/object_name_index.py). GROUP_EXCHANGE mailboxes have no
    # Contacts concept at all, so there's no alias to add for them;
    # ContactProvider construction for that sub_type simply finds no
    # object-name index entry, degrading the same way as any other
    # unrecognized version.
    object_names={_CONTACT_TABLE: ("contact_db",)},
)


#: Constructor-style factory over ``CONTACT_CONFIG`` — see
#: ``make_saas_provider``'s own docstring for what "constructor-style
#: factory" and ``shared`` mean.
ContactProvider = make_saas_provider(CONTACT_CONFIG, name="ContactProvider")
