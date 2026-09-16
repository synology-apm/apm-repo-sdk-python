"""``MailProvider``/``ArchiveMailProvider``: M365/GWS Mail via
``mail_table`` + the shared ``X-ABL-ID`` fragment-reassembly engine (see
``build_eml``), built as a ``SaasWorkloadProvider`` config. Both platforms
share this module's tree/reassembly code — the ``content_list``/META JSON
shape (``fragment_id``/``type``/``file_name``/``content_id``/``object_id``/
``size``) is close enough that one code path covers both.

M365's ``mail_table.mail_id`` has no ``UNIQUE`` constraint (multiple
historical version rows can coexist, and the current tree/listing query
does not de-duplicate or tie-break between them); GWS's does (single
current row only).

M365 Mail's tree is normally ``RecursiveGroupFlatTree`` over
``mail_folder_table``'s own real, nested folder hierarchy
(``_open_m365_folder_tree``) — every real folder shows up, including one
with zero backed-up messages. That degrades to the older, flat,
item-driven ``SyntheticGroupedTree`` (folder names resolved separately via
``_m365_folder_names``) whenever the hierarchy table isn't indexed or
doesn't validate — see ``_open_m365_folder_tree``'s own docstring for
every such case. GWS has no folder hierarchy at all, only many-to-many
labels, surfaced as an ``extra_attrs`` field rather than a tree grouping
(``_gws_mail_labels``), so it always uses the flat tree. Archive Mail is a
real, separate M365-only mailbox with a schema byte-for-byte identical to
regular Mail's, resolved via the object-name index alone — see
``ArchiveMailProvider``.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable

from ...errors import DataCorruptError
from ...storage.table import Column, Table
from ..base import RestorableUnit, UnitKind
from ..content.saas_artifact import LazyArtifact
from ..content.saas_mail import build_eml
from .object_name_index import read_grouped_names, read_id_to_name_map
from .objectdb import read_object
from .provider import (
    SaasWorkloadConfig,
    SaasWorkloadProvider,
    extras_attr,
    group_display_name_resolver,
    make_saas_provider,
)
from .tree_strategy import RecursiveGroupFlatTree, SyntheticGroupedTree, TreeStrategy

_MAIL_TABLE = "mail_table"
_MAIL_FOLDER_TABLE = "mail_folder_table"  # M365 only — see _open_m365_folder_tree()/_m365_folder_names()
# GWS only. The *same* real table name as the definitions above ("label"
# instead of "folder" — Gmail has no folder hierarchy at all, only
# many-to-many labels) but ambiguously shared between two different real
# meanings depending on which db holds it: mail_db's own copy is the
# mail<->label *membership* join (mail_id, label_id); mail_label_db's own
# copy is the label *definitions* (label_id, label_name) — see
# _gws_mail_labels()'s own docstring for why a schema-based scan for
# either one would have been fundamentally unreliable, not just slower.
_MAIL_LABEL_TABLE = "mail_label_table"
# Catalog names for mail_folder_db's own object_id, in try-order
# (USER_EXCHANGE and GROUP_EXCHANGE respectively) — see
# _folder_names()'s own docstring.
_MAIL_FOLDER_DB_NAMES = ("mail_folder_db", "group_mail_folder_db")
# Archive Mail (M365 only — see ARCHIVE_MAIL_CONFIG) has its own,
# separate folder db under this one name — same schema, table name, and
# column set as mail_folder_db's own.
_ARCHIVE_MAIL_FOLDER_DB_NAMES = ("archive_mail_folder_db",)
# Archive Mail's own mail_table — same schema as regular Mail's (module
# docstring), implemented generically from that schema regardless of
# whether any given account's Archive folder is ever populated, the
# same precedent Teams/Chat's own not-yet-populated message content
# follows.
_ARCHIVE_MAIL_DB_NAMES = ("archive_mail_db",)
_MAIL_LABEL_DB_NAMES = ("mail_label_db",)
# The *membership* half of GWS labels lives inside mail_db itself, not
# mail_label_db — see _MAIL_LABEL_TABLE's own comment.
_MAIL_DB_NAMES_FOR_LABEL_MEMBERSHIP = ("mail_db",)
_SKEL_TYPE = 0  # the EML skeleton itself, never a splice target
_ALL_MAIL_GROUP = "Mail"
_ARCHIVE_MAIL_GROUP = "Archive"

_Row = dict[str, object | None]
_Key = tuple[str, ...]

_MAIL_COLUMNS = [
    Column("mail_id"),
    Column("subject"),
    Column("meta_object_id"),
    Column("parent_folder_id", required=False),
]

# folder_id/folder_name/parent_folder_id/is_root — see
# _open_m365_folder_tree's own docstring. Used only by the hierarchy
# path: _folder_names's own flat-lookup degrade path is separate
# machinery (read_id_to_name_map, its own 2-column folder_id/folder_name
# lookup) and never touches this constant. is_root must still be
# declared here — root-anchor resolution below reads it, and Table only
# ever selects columns it was declared with.
_MAIL_FOLDER_COLUMNS = [
    Column("folder_id"),
    Column("folder_name"),
    Column("parent_folder_id"),
    Column("is_root"),
]

# A missing subject is real, expected data, not a corruption signal —
# shown literally, never a raw internal id (the M365/GWS message id,
# meaningless to a user) standing in for a name.
_NO_SUBJECT_LABEL = "(no subject)"


async def _folder_names(provider: SaasWorkloadProvider, object_names: tuple[str, ...]) -> dict[str, str] | None:
    """Best-effort ``folder_id -> display name`` map from the object-name
    index (``read_id_to_name_map`` against ``object_names`` — see
    ``_m365_folder_names``/``_archive_folder_names``); ``None`` if the
    index lacks it, in which case the caller falls back to
    ``SyntheticGroupedTree``'s default of the raw ``parent_folder_id``
    as the group's name. Deliberately optional, not part of
    ``MAIL_CONFIG.tables`` — a stream missing this table should still
    browse mail by folder id rather than degrade entirely to
    ``RawObjectProvider`` over metadata no single mail's content
    actually requires.

    Only ever reached as part of the *degrade* path now
    (``_open_m365_folder_tree`` returning ``None``) — the normal case
    resolves folder names straight off ``mail_folder_table``'s own rows,
    with no separate lookup at all.

    No scan of any kind: ``mail_folder_db`` also holds
    ``config_table``/``mail_change_table``/``folder_recovery_table`` in
    real data and can embed more than one snapshot, and Archive Mail's
    own ``mail_folder_table`` is the same schema as regular Mail's — a
    schema-only scan for "an object with ``mail_folder_table``" would
    have no way to pick the right one; only the index's own naming
    (``object_names``) can."""
    return await read_id_to_name_map(
        provider.dedup_file,
        provider.object_name_index,
        object_names,
        _MAIL_FOLDER_TABLE,
        id_column="folder_id",
        name_column="folder_name",
    )


async def _m365_folder_names(provider: SaasWorkloadProvider) -> dict[str, str] | None:
    """Regular M365 Mail's folder names — see ``_folder_names``.
    Includes the mailbox root (``msgfolderroot``) with no
    special-casing: the connector's own table names it like any other
    row, with a real, correctly localized label."""
    return await _folder_names(provider, _MAIL_FOLDER_DB_NAMES)


async def _archive_folder_names(provider: SaasWorkloadProvider) -> dict[str, str] | None:
    """Archive Mail's own equivalent of ``_m365_folder_names`` —
    ``archive_mail_folder_db`` rather than ``mail_folder_db`` — see
    ``_folder_names``."""
    return await _folder_names(provider, _ARCHIVE_MAIL_FOLDER_DB_NAMES)


async def _root_folder_id(table: Table) -> str | None:
    """The one row's own ``folder_id`` where ``is_root`` is truthy — the
    value every top-level folder's ``parent_folder_id`` equals, and the
    synthetic anchor ``RecursiveGroupFlatTree`` starts recursion from.
    ``None`` if no such row exists (malformed data), the one validation
    ``_open_m365_folder_tree`` needs beyond schema shape."""
    row = await table.select_one("is_root = ?", (1,))
    return str(row["folder_id"]) if row is not None else None


async def _open_m365_folder_tree(provider: SaasWorkloadProvider) -> RecursiveGroupFlatTree | None:
    """Opens M365 Mail/Archive Mail's real ``mail_folder_table`` hierarchy
    as a ``RecursiveGroupFlatTree`` — the primary tree source whenever it
    resolves and validates. ``None`` on any of three legitimate degrade
    cases (never a crash), each falling back to the older flat,
    item-driven ``SyntheticGroupedTree``/``_m365_folder_names`` path:
    the table isn't indexed for this version at all, its real schema is
    missing a column this needs (an older connector version — caught as
    ``DataCorruptError``, the same failure ``Table.create`` raises for any
    other missing-required-column case), or it has no ``is_root`` row to
    anchor recursion at.

    No cycle detection for a ``parent_folder_id`` cycle: ``RecursiveTree``
    (Drive) and ``NamedGroupRecursiveTree`` (Site) trust their own real
    parent-pointer data the same way — real cloud-API-sourced folder
    hierarchies don't cycle."""
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
        # Newest-first: a real inbox's natural default view is
        # most-recent message first, not oldest-first.
        descending=True,
    )


async def _gws_mail_labels(provider: SaasWorkloadProvider) -> dict[str, list[str]] | None:
    """Best-effort ``mail_id -> [real label names]`` map for GWS's own
    label mechanism (see this module's own docstring for why Gmail has
    no folder hierarchy) — surfaced as an ``extra_attrs`` field
    (``MAIL_CONFIG``), not a tree grouping the way M365's real folders
    are.

    ``mail_label_table`` means two different real tables depending on
    which db holds it: the membership join (``row_id, mail_id,
    label_id``) inside ``mail_db``, or the label definitions
    (``row_id, label_id, label_name, label_type``) inside
    ``mail_label_db`` — resolved via the object-name index only, since a
    schema-only scan for ``mail_label_table`` couldn't tell which one
    it found; ``read_grouped_names`` resolves each half separately for
    exactly this reason."""
    return await read_grouped_names(
        provider.dedup_file,
        provider.object_name_index,
        definition_names=_MAIL_LABEL_DB_NAMES,
        definition_table=_MAIL_LABEL_TABLE,
        id_column="label_id",
        name_column="label_name",
        membership_names=_MAIL_DB_NAMES_FOR_LABEL_MEMBERSHIP,
        membership_table=_MAIL_LABEL_TABLE,
        item_column="mail_id",
        group_column="label_id",
    )


def _mail_extra_attrs(provider: SaasWorkloadProvider, row: _Row) -> dict[str, object]:
    return extras_attr(provider, "gws_mail_labels", row["mail_id"], "labels")


def _mail_display_name(row: _Row) -> str:
    """Display name for a mail: ``row["subject"]`` if non-empty, else
    ``_NO_SUBJECT_LABEL`` — covers both a real empty-string subject and an
    unconfirmed-but-possible SQL ``NULL``."""
    subject = row.get("subject")
    return str(subject) if subject else _NO_SUBJECT_LABEL


def _make_build_tree(
    folder_names_resolver: Callable[[SaasWorkloadProvider], Awaitable[dict[str, str] | None]],
    root_name: str,
    *,
    resolve_gws_labels: bool,
) -> Callable[[SaasWorkloadProvider], Awaitable[TreeStrategy]]:
    """Builds one ``tree_factory`` bound to ``folder_names_resolver``/
    ``root_name`` — ``MAIL_CONFIG`` and ``ARCHIVE_MAIL_CONFIG`` each get
    their own, so they never cross-contaminate which index
    names they resolve folder names via. ``resolve_gws_labels`` is
    ``False`` for Archive Mail: Archive is M365-only (GWS's own
    ``additional_meta`` never has an ``archive_mail_db`` entry), so
    there is nothing to prefetch there."""

    async def _build_tree(provider: SaasWorkloadProvider) -> TreeStrategy:
        # I/O only for M365 (a real service-DB read, or two on the
        # degrade path below) and GWS (two real service-DB reads for
        # labels) — GWS mail otherwise falls through group_column=None
        # to the single synthetic root_name group unconditionally.
        is_m365 = provider.is_m365
        folder_names: dict[str, str] | None = None
        if is_m365:
            hierarchy_tree = await _open_m365_folder_tree(provider)
            if hierarchy_tree is not None:
                return hierarchy_tree
            folder_names = await folder_names_resolver(provider)
        elif resolve_gws_labels:
            provider.extras["gws_mail_labels"] = await _gws_mail_labels(provider) or {}

        return SyntheticGroupedTree(
            provider,
            table=_MAIL_TABLE,
            columns=_MAIL_COLUMNS,
            id_column="mail_id",
            # GWS has no folder hierarchy at all (only many-to-many
            # labels — see this module's own docstring) — group_column=
            # None means every message sits in the one synthetic
            # root_name group, queried without any WHERE at all.
            group_column="parent_folder_id" if is_m365 else None,
            display_name=_mail_display_name,
            root_name=root_name,
            # remote_timestamp (received/sent time) is real on both
            # platforms and indexed (remote_timestamp_index) — a closer
            # match to "inbox sorted by time" than subject would be, but
            # not declared in _MAIL_COLUMNS at all —
            # tree_strategy.py's _resolve_order_by()
            # falls back to "subject" (always present) if a given schema
            # genuinely lacks it, then SQLite's own implicit ``rowid`` as
            # the deterministic pagination tiebreaker regardless.
            order_by=["remote_timestamp", "subject"],
            # Newest-first: a real inbox's natural default view is
            # most-recent message first, not oldest-first.
            descending=True,
            group_display_name=group_display_name_resolver(folder_names),
        )

    return _build_tree


_build_tree = _make_build_tree(_m365_folder_names, _ALL_MAIL_GROUP, resolve_gws_labels=True)
_build_archive_tree = _make_build_tree(_archive_folder_names, _ARCHIVE_MAIL_GROUP, resolve_gws_labels=False)


def _declared_size(entry: dict[str, object]) -> int | None:
    """A content_list entry's own ``size`` field, narrowed to ``int`` for
    ``read_object``'s ``expected_size`` — real META always has an int
    here, but this is parsed JSON, so a caller can't assume it without
    checking."""
    size = entry.get("size")
    return size if isinstance(size, int) else None


async def _assemble_eml(provider: SaasWorkloadProvider, meta_object_id: str) -> bytes:
    object_db = provider.object_db(_MAIL_TABLE)
    meta_bytes = await read_object(object_db, provider.dedup_file, meta_object_id)
    try:
        meta = json.loads(meta_bytes)
    except json.JSONDecodeError as exc:
        raise DataCorruptError(f"mail META {meta_object_id!r} did not parse as JSON: {exc}") from exc
    content_list = meta.get("content_list") or []

    skel_entry = next((c for c in content_list if c.get("type") == _SKEL_TYPE), None)
    if skel_entry is None:
        raise DataCorruptError(f"mail META {meta_object_id!r} has no skeleton (type=0) fragment in content_list")
    skel_bytes = await read_object(
        object_db, provider.dedup_file, skel_entry["object_id"], expected_size=_declared_size(skel_entry)
    )

    fragments_by_id: dict[str, bytes] = {}
    for entry in content_list:
        if entry.get("type") == _SKEL_TYPE:
            continue
        fragment_id = entry.get("fragment_id")
        object_id = entry.get("object_id")
        if not fragment_id or not object_id:  # pragma: no cover - defensive: real META always has both
            continue
        fragments_by_id[fragment_id] = await read_object(
            object_db, provider.dedup_file, object_id, expected_size=_declared_size(entry)
        )

    return build_eml(skel_bytes, fragments_by_id)


async def _assemble(provider: SaasWorkloadProvider, row: _Row, key: _Key) -> RestorableUnit:
    # No I/O here — the real reads happen inside the LazyArtifact's own
    # (awaited-at-most-once) build callback.
    meta_object_id = str(row["meta_object_id"])
    name = _mail_display_name(row)
    return RestorableUnit(
        ref=provider.ref_for(key),
        name=f"{name}.eml",
        is_leaf=True,
        kind=UnitKind.MAIL,
        content=LazyArtifact(lambda: _assemble_eml(provider, meta_object_id)),
    )


#: ``SaasWorkloadConfig`` behind ``MailProvider`` — regular Mail,
#: resolved via ``mail_db``/``group_mail_db``.
MAIL_CONFIG = SaasWorkloadConfig(
    root_name=_ALL_MAIL_GROUP,
    leaf_kind=UnitKind.MAIL,
    tables=(_MAIL_TABLE,),
    tree_factory=_build_tree,
    assemble=_assemble,
    extra_attrs=_mail_extra_attrs,
    # "mail_db" for USER_EXCHANGE, "group_mail_db" for GROUP_EXCHANGE.
    # GWS has no GROUP_EXCHANGE equivalent — see units/dispatch.py's own
    # module-level candidate table.
    object_names={
        _MAIL_TABLE: ("mail_db", "group_mail_db"),
        # Optional (not in tables=) — _open_m365_folder_tree resolves it
        # lazily, on GWS a no-op since it's never even attempted there.
        _MAIL_FOLDER_TABLE: _MAIL_FOLDER_DB_NAMES,
    },
)

#: ``SaasWorkloadConfig`` behind ``ArchiveMailProvider`` — M365's separate
#: ``archive_mail_db`` mailbox.
ARCHIVE_MAIL_CONFIG = SaasWorkloadConfig(
    root_name=_ARCHIVE_MAIL_GROUP,
    leaf_kind=UnitKind.MAIL,
    tables=(_MAIL_TABLE,),
    tree_factory=_build_archive_tree,
    assemble=_assemble,
    object_names={
        _MAIL_TABLE: _ARCHIVE_MAIL_DB_NAMES,
        _MAIL_FOLDER_TABLE: _ARCHIVE_MAIL_FOLDER_DB_NAMES,
    },
    # Same schema as mail_db — resolved via object-name index only (module
    # docstring). If the object-name index doesn't have archive_mail_db,
    # Archive is simply absent from this version's tree.
)


#: Constructor-style factory over ``MAIL_CONFIG`` — see
#: ``make_saas_provider``'s own docstring for what "constructor-style
#: factory" and ``shared`` mean.
MailProvider = make_saas_provider(MAIL_CONFIG, name="MailProvider")

#: M365's Archive mailbox — a real, separate ``mail_table`` coexisting with
#: regular Mail in the same ``USER_EXCHANGE`` version — see this module's
#: own module docstring for its schema, and ``_ARCHIVE_MAIL_DB_NAMES``'s
#: own comment for why it's implemented generically from that schema
#: alone.
#:
#: Raises ``UnsupportedDataFormatError`` whenever the object-name index doesn't
#: resolve ``archive_mail_db`` for this version — there is no scan
#: fallback — so ``units/dispatch.py``'s "try every candidate" dispatch
#: simply omits Archive from that version's sibling set rather than
#: risk cross-attributing mail between the two. Not offered for
#: ``GROUP_EXCHANGE``: its own ``additional_meta`` never has an
#: ``archive_mail_db`` entry.
ArchiveMailProvider = make_saas_provider(ARCHIVE_MAIL_CONFIG, name="ArchiveMailProvider")
