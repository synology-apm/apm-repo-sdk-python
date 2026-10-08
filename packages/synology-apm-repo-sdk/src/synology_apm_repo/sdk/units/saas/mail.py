"""``open_mail_provider``/``open_archive_mail_provider``: M365/GWS Mail via
``mail_table``, each message reassembled into an ``.eml`` from its META
``content_list`` fragments (``build_eml``). One code path covers both
platforms.

M365's ``mail_table.mail_id`` is not unique (historical rows can coexist,
and listings don't de-duplicate them); GWS's is.

M365's tree is ``mail_folder_table``'s folder hierarchy, empty folders
included, falling back to a flat folder grouping when that table is
unusable (``_open_m365_folder_tree``). GWS has no folders, only
many-to-many labels shown as a ``details`` entry, so its messages sit
in one flat group. Archive Mail is a separate M365-only mailbox with
regular Mail's schema (``open_archive_mail_provider``).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable, Mapping

from ..._util.jsonparse import parse_json_object
from ...errors import DataCorruptError
from ...storage.table import Column, Table
from ...units.provider_kit import mtime_from_raw
from ..base import ItemColumns, UnitKind
from ..content.saas_artifact import LazyArtifact
from ..content.saas_mail import build_eml
from .provider import NodeExtras, SaasWorkloadConfig, SaasWorkloadProvider, make_saas_provider
from .tree_strategy import Key, RecursiveGroupFlatTree, Row, SyntheticGroupedTree, TreeStrategy
from .workload_helpers import group_display_name_resolver, membership_detail

_MAIL_TABLE = "mail_table"
_MAIL_FOLDER_TABLE = "mail_folder_table"  # M365 only
# GWS only, with two meanings by db: in mail_db the mail<->label
# membership join (mail_id, label_id); in mail_label_db the label
# definitions (label_id, label_name).
_MAIL_LABEL_TABLE = "mail_label_table"
# Index names for the folder db, in try-order (USER_EXCHANGE, GROUP_EXCHANGE).
_MAIL_FOLDER_DB_NAMES = ("mail_folder_db", "group_mail_folder_db")
_ARCHIVE_MAIL_FOLDER_DB_NAMES = ("archive_mail_folder_db",)
_ARCHIVE_MAIL_DB_NAMES = ("archive_mail_db",)
_MAIL_LABEL_DB_NAMES = ("mail_label_db",)
_MAIL_DB_NAMES_FOR_LABEL_MEMBERSHIP = ("mail_db",)
_SKEL_TYPE = 0  # the EML skeleton itself, never a splice target
_ALL_MAIL_GROUP = "Mail"
_ARCHIVE_MAIL_GROUP = "Archive"


@dataclasses.dataclass(frozen=True, slots=True)
class MailState:
    """What a mail ``tree_factory`` prefetches for ``_mail_extras``.

    Attributes:
        gws_labels: GWS label names, by ``mail_id``; empty for M365.
    """

    gws_labels: Mapping[str, list[str]] = dataclasses.field(default_factory=dict)


_Provider = SaasWorkloadProvider[MailState]


_MAIL_COLUMNS = [
    Column("mail_id"),
    Column("subject"),
    Column("meta_object_id"),
    Column("parent_folder_id", required=False),
    Column("sender", required=False),  # a display name; no address is stored
    # Received/sent time; absent in some older schemas.
    Column("remote_timestamp", required=False),
]

# For the folder-hierarchy tree; is_root is read by _root_folder_id.
_MAIL_FOLDER_COLUMNS = [
    Column("folder_id"),
    Column("folder_name"),
    Column("parent_folder_id"),
    Column("is_root"),
]


_NO_SUBJECT_LABEL = "(no subject)"


async def _folder_names(provider: _Provider, object_names: tuple[str, ...]) -> dict[str, str] | None:
    """Best-effort ``folder_id -> display name`` map for the flat fallback
    tree, read from ``object_names``' folder db. ``None`` if unavailable;
    groups then show the raw folder id."""
    return await provider.read_id_to_name_map(
        object_names, _MAIL_FOLDER_TABLE, id_column="folder_id", name_column="folder_name"
    )


async def _m365_folder_names(provider: _Provider) -> dict[str, str] | None:
    """Regular M365 Mail's folder names — see ``_folder_names``."""
    return await _folder_names(provider, _MAIL_FOLDER_DB_NAMES)


async def _archive_folder_names(provider: _Provider) -> dict[str, str] | None:
    """Archive Mail's folder names — see ``_folder_names``."""
    return await _folder_names(provider, _ARCHIVE_MAIL_FOLDER_DB_NAMES)


async def _root_folder_id(table: Table) -> str | None:
    """The ``is_root`` row's ``folder_id`` — every top-level folder's
    ``parent_folder_id`` — or ``None`` if there's no such row."""
    row = await table.select_one("is_root = ?", (1,))
    return str(row["folder_id"]) if row is not None else None


async def _open_m365_folder_tree(provider: _Provider) -> RecursiveGroupFlatTree | None:
    """M365 Mail/Archive Mail's ``mail_folder_table`` hierarchy as a
    ``RecursiveGroupFlatTree``. ``None`` — the caller falls back to the
    flat tree — when the table isn't indexed, lacks a needed column (an
    older connector), or has no ``is_root`` row. Folder cycles aren't
    detected."""
    conn = await provider.open_optional_table_via_index(_MAIL_FOLDER_TABLE)
    if conn is None:
        return None
    try:
        table = await Table.create(conn, _MAIL_FOLDER_TABLE, _MAIL_FOLDER_COLUMNS)
    except DataCorruptError:
        return None
    root_id = await _root_folder_id(table)
    if root_id is None:
        return None
    return RecursiveGroupFlatTree(
        provider,
        group_table=_MAIL_FOLDER_TABLE,
        group_columns=_MAIL_FOLDER_COLUMNS,
        group_id_column="folder_id",
        group_name_column="folder_name",
        group_parent_column="parent_folder_id",
        group_root_id=root_id,
        leaf_table=_MAIL_TABLE,
        leaf_columns=_MAIL_COLUMNS,
        leaf_id_column="mail_id",
        leaf_group_column="parent_folder_id",
        display_name=_mail_display_name,
        order_by=["remote_timestamp", "subject"],
        descending=True,  # newest first
    )


async def _gws_mail_labels(provider: _Provider) -> dict[str, list[str]] | None:
    """Best-effort ``mail_id -> [label names]`` map for GWS, joining
    ``mail_label_db``'s definitions with ``mail_db``'s membership
    table."""
    return await provider.read_grouped_names(
        definition_names=_MAIL_LABEL_DB_NAMES,
        definition_table=_MAIL_LABEL_TABLE,
        id_column="label_id",
        name_column="label_name",
        membership_names=_MAIL_DB_NAMES_FOR_LABEL_MEMBERSHIP,
        membership_table=_MAIL_LABEL_TABLE,
        item_column="mail_id",
        group_column="label_id",
    )


def _common_mail_extras(row: Row, details: dict[str, object]) -> NodeExtras:
    """``sender`` and ``mtime``, shared by regular Mail and Archive Mail."""
    sender = row.get("sender")
    return NodeExtras(
        mtime=mtime_from_raw(row.get("remote_timestamp")),
        columns=ItemColumns(sender=str(sender) if sender else None),
        details=details,
    )


def _mail_extras(provider: _Provider, row: Row) -> NodeExtras:
    return _common_mail_extras(row, membership_detail(provider.state.gws_labels, row["mail_id"], "labels"))


def _archive_mail_extras(provider: _Provider, row: Row) -> NodeExtras:
    del provider  # M365-only: no GWS labels
    return _common_mail_extras(row, {})


def _mail_display_name(row: Row) -> str:
    """``row["subject"]``, or ``_NO_SUBJECT_LABEL`` when empty or NULL."""
    subject = row.get("subject")
    return str(subject) if subject else _NO_SUBJECT_LABEL


def _make_build_tree(
    folder_names_resolver: Callable[[_Provider], Awaitable[dict[str, str] | None]],
    root_name: str,
    *,
    resolve_gws_labels: bool,
) -> Callable[[_Provider], Awaitable[tuple[TreeStrategy, MailState]]]:
    """One ``tree_factory`` for ``MAIL_CONFIG`` or ``ARCHIVE_MAIL_CONFIG``.
    ``resolve_gws_labels`` is ``False`` for the M365-only Archive Mail."""

    async def _build_tree(provider: _Provider) -> tuple[TreeStrategy, MailState]:
        is_m365 = provider.is_m365
        folder_names: dict[str, str] | None = None
        labels: dict[str, list[str]] = {}
        if is_m365:
            hierarchy_tree = await _open_m365_folder_tree(provider)
            if hierarchy_tree is not None:
                return hierarchy_tree, MailState()
            folder_names = await folder_names_resolver(provider)
        elif resolve_gws_labels:
            labels = await _gws_mail_labels(provider) or {}

        tree = SyntheticGroupedTree(
            provider,
            table=_MAIL_TABLE,
            columns=_MAIL_COLUMNS,
            id_column="mail_id",
            # None (GWS): every message in the one root_name group.
            group_column="parent_folder_id" if is_m365 else None,
            display_name=_mail_display_name,
            root_name=root_name,
            order_by=["remote_timestamp", "subject"],
            descending=True,  # newest first
            group_display_name=group_display_name_resolver(folder_names),
        )
        return tree, MailState(gws_labels=labels)

    return _build_tree


_build_tree = _make_build_tree(_m365_folder_names, _ALL_MAIL_GROUP, resolve_gws_labels=True)
_build_archive_tree = _make_build_tree(_archive_folder_names, _ARCHIVE_MAIL_GROUP, resolve_gws_labels=False)


def _declared_size(entry: dict[str, object]) -> int | None:
    """A content_list entry's ``size``, or ``None`` when it isn't an int."""
    size = entry.get("size")
    return size if isinstance(size, int) else None


async def _assemble_eml(provider: _Provider, meta_object_id: str) -> bytes:
    meta_bytes = await provider.read_object(_MAIL_TABLE, meta_object_id)
    meta = parse_json_object(meta_bytes, f"mail META {meta_object_id!r}", ref=meta_object_id)
    content_list = meta.get("content_list") or []

    skel_entry = next((c for c in content_list if c.get("type") == _SKEL_TYPE), None)
    if skel_entry is None:
        raise DataCorruptError(f"mail META {meta_object_id!r} has no skeleton (type=0) fragment in content_list")
    skel_bytes = await provider.read_object(
        _MAIL_TABLE, skel_entry["object_id"], expected_size=_declared_size(skel_entry)
    )

    fragments_by_id: dict[str, bytes] = {}
    for entry in content_list:
        if entry.get("type") == _SKEL_TYPE:
            continue
        fragment_id = entry.get("fragment_id")
        object_id = entry.get("object_id")
        if not fragment_id or not object_id:  # pragma: no cover - defensive: real META always has both
            continue
        fragments_by_id[fragment_id] = await provider.read_object(
            _MAIL_TABLE, object_id, expected_size=_declared_size(entry)
        )

    return build_eml(skel_bytes, fragments_by_id)


def _export_name(provider: _Provider, row: Row) -> str:
    del provider  # the name comes from the row alone
    return f"{_mail_display_name(row)}.eml"


async def _content(provider: _Provider, row: Row, key: Key) -> LazyArtifact:
    meta_object_id = str(row["meta_object_id"])
    return LazyArtifact(lambda: _assemble_eml(provider, meta_object_id))


#: ``SaasWorkloadConfig`` behind ``open_mail_provider`` — regular Mail,
#: resolved via ``mail_db``/``group_mail_db``.
MAIL_CONFIG = SaasWorkloadConfig(
    root_name=_ALL_MAIL_GROUP,
    leaf_kind=UnitKind.MAIL,
    tables=(_MAIL_TABLE,),
    tree_factory=_build_tree,
    content=_content,
    leaf_export_name=_export_name,
    leaf_extras=_mail_extras,
    object_names={
        # "mail_db" for USER_EXCHANGE, "group_mail_db" for GROUP_EXCHANGE.
        _MAIL_TABLE: ("mail_db", "group_mail_db"),
        # Optional (not in tables): opened only by _open_m365_folder_tree.
        _MAIL_FOLDER_TABLE: _MAIL_FOLDER_DB_NAMES,
    },
)

#: ``SaasWorkloadConfig`` behind ``open_archive_mail_provider`` — M365's separate
#: ``archive_mail_db`` mailbox.
ARCHIVE_MAIL_CONFIG = SaasWorkloadConfig(
    root_name=_ARCHIVE_MAIL_GROUP,
    leaf_kind=UnitKind.MAIL,
    tables=(_MAIL_TABLE,),
    tree_factory=_build_archive_tree,
    content=_content,
    leaf_export_name=_export_name,
    leaf_extras=_archive_mail_extras,
    object_names={
        _MAIL_TABLE: _ARCHIVE_MAIL_DB_NAMES,
        _MAIL_FOLDER_TABLE: _ARCHIVE_MAIL_FOLDER_DB_NAMES,
    },
)


#: Async factory over ``MAIL_CONFIG`` (see ``make_saas_provider``).
open_mail_provider = make_saas_provider(MAIL_CONFIG, name="open_mail_provider")

#: Async factory over ``ARCHIVE_MAIL_CONFIG``: M365's Archive mailbox, a
#: separate ``mail_table`` beside regular Mail in a ``USER_EXCHANGE``
#: version. Raises ``UnsupportedDataFormatError`` when the index has no
#: ``archive_mail_db``, so dispatch leaves Archive out of that version.
open_archive_mail_provider = make_saas_provider(ARCHIVE_MAIL_CONFIG, name="open_archive_mail_provider")
