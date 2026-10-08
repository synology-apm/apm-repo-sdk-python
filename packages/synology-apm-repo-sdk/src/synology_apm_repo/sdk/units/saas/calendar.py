"""``open_calendar_provider``: M365/GWS Calendar via ``calendar_table`` (the
calendar list) and ``calendar_event_table`` (its events), two separately
indexed service DBs.

- Tree: My/Other Calendars (``CategorizedGroupTree``, split by
  ``_is_other_calendar``) → calendar → event (``NamedGroupFlatTree``).
- Content: each event exports as ``.ics``, built by ``build_ics`` from its
  META object's ``client_metadata`` alone; recurring series export their
  ``RRULE`` and detached occurrences their ``RECURRENCE-ID``.
- A calendar's displayed name may differ from ``calendar_name``
  (``_group_name_override``).
"""

from __future__ import annotations

from collections.abc import Callable

from ..._util.jsonparse import try_parse_json_object
from ...storage.table import Column, Table
from ...units.provider_kit import mtime_from_raw
from ..base import ItemColumns, UnitKind
from ..content.saas_artifact import LazyArtifact
from ..content.saas_calendar import M365_RECURRENCE_FREQ, build_ics
from .provider import NodeExtras, SaasWorkloadConfig, SaasWorkloadProvider, make_saas_provider
from .tree_strategy import CategorizedGroupTree, Key, NamedGroupFlatTree, Row
from .workload_helpers import owning_account_user_info

_CALENDAR_TABLE = "calendar_table"
_EVENT_TABLE = "calendar_event_table"


_Provider = SaasWorkloadProvider[None]


#: Key tokens of the My/Other Calendars categories; ``_CATEGORY_LABELS``
#: holds their displayed names.
_CATEGORY_MY = "my"
_CATEGORY_OTHER = "other"
_CATEGORY_LABELS = {_CATEGORY_MY: "My Calendars", _CATEGORY_OTHER: "Other Calendars"}

_CALENDAR_COLUMNS = [
    Column("calendar_id"),
    Column("calendar_name"),
    Column("timezone", required=False),
    Column("calendar_type", required=False),  # see _is_other_calendar
    # The user's own relabeling (Google's "summaryOverride").
    Column("calendar_name_override", required=False),
]

_NO_TITLE_LABEL = "(no title)"
_EVENT_COLUMNS = [
    Column("event_id"),
    Column("calendar_id"),
    Column("summary"),
    Column("meta_object_id"),
    Column("event_start_time", required=False),
    Column("event_end_time", required=False),
    # A Graph-style recurrence pattern JSON on both platforms.
    Column("recurrence_rule", required=False),
]


def _event_display_name(row: Row) -> str:
    """``row["summary"]``, or ``_NO_TITLE_LABEL`` when empty or NULL."""
    summary = row.get("summary")
    return str(summary) if summary else _NO_TITLE_LABEL


def _recurrence_label(row: Row) -> str:
    """A short recurrence label from ``recurrence_rule``'s
    ``pattern.type`` ("Daily", "Weekly", ...; "Recurring" for an unknown
    type), blank for a one-off event or an unparseable rule."""
    raw = row.get("recurrence_rule")
    if not raw:
        return ""
    parsed = try_parse_json_object(str(raw))
    pattern = parsed.get("pattern") if parsed is not None else None
    if not isinstance(pattern, dict):
        return ""
    pattern_type = pattern.get("type")
    freq = M365_RECURRENCE_FREQ.get(str(pattern_type))
    return freq.capitalize() if freq else "Recurring"


def _event_extras(provider: _Provider, row: Row) -> NodeExtras:
    del provider
    return NodeExtras(
        columns=ItemColumns(
            event_start=mtime_from_raw(row.get("event_start_time")),
            event_end=mtime_from_raw(row.get("event_end_time")),
            recurrence=_recurrence_label(row),
        )
    )


def _group_extras(provider: _Provider, key: Key) -> NodeExtras:
    # The root and the My/Other categories hold only containers, so they
    # report CATEGORY_GROUP rather than CALENDAR_EVENT as their leaf kind.
    del provider
    return NodeExtras(leaf_kind=UnitKind.CATEGORY_GROUP) if len(key) < 2 else NodeExtras()


def _group_name_override(owning_email: str | None, owning_name: str | None) -> Callable[[Row], str | None]:
    """A calendar's displayed-name override: ``calendar_name_override``
    when set, else the account's name for its primary calendar (whose
    ``calendar_id`` is the account email, and whose ``calendar_name``
    defaults to that email), else ``None`` (use ``calendar_name``)."""

    def _override(row: Row) -> str | None:
        explicit = row.get("calendar_name_override")
        if explicit:
            return str(explicit)
        if owning_email and owning_name and str(row.get("calendar_id")) == owning_email:
            return owning_name
        return None

    return _override


def _is_other_calendar(calendar_row: Row) -> bool:
    # 1: a subscribed calendar (e.g. "Holidays in ..."); 0 or None
    # (M365, which has no such distinction): the account's own.
    return calendar_row.get("calendar_type") == 1


async def _build_tree(provider: _Provider) -> tuple[CategorizedGroupTree, None]:
    # Every calendar's category up front; bounded by the calendar count.
    calendar_table = await Table.create(provider.table(_CALENDAR_TABLE), _CALENDAR_TABLE, _CALENDAR_COLUMNS)
    calendar_categories: dict[str, str] = {}
    async for row in calendar_table.select():
        calendar_id = str(row["calendar_id"])
        calendar_categories[calendar_id] = _CATEGORY_OTHER if _is_other_calendar(row) else _CATEGORY_MY

    owning_user_info = await owning_account_user_info(provider.repo, provider.version)
    owning_email = str(owning_user_info["email"]) if owning_user_info and owning_user_info.get("email") else None
    owning_name = str(owning_user_info["name"]) if owning_user_info and owning_user_info.get("name") else None

    inner = NamedGroupFlatTree(
        provider,
        group_table=_CALENDAR_TABLE,
        group_columns=_CALENDAR_COLUMNS,
        group_id_column="calendar_id",
        group_name_column="calendar_name",
        group_name_override=_group_name_override(owning_email, owning_name),
        leaf_table=_EVENT_TABLE,
        leaf_columns=_EVENT_COLUMNS,
        leaf_id_column="event_id",
        leaf_group_column="calendar_id",
        display_name=_event_display_name,
        # The Portal's own event sort column.
        order_by=["event_start_time", "summary"],
    )
    return CategorizedGroupTree(inner, categories=calendar_categories, labels=_CATEGORY_LABELS), None


def _export_name(provider: _Provider, row: Row) -> str:
    del provider  # the name comes from the row alone
    return f"{_event_display_name(row)}.ics"


async def _content(provider: _Provider, row: Row, key: Key) -> LazyArtifact:
    meta_object_id = str(row["meta_object_id"])
    event_id = str(row["event_id"])

    async def _build() -> bytes:
        return build_ics(await provider.read_object(_EVENT_TABLE, meta_object_id), event_id)

    return LazyArtifact(_build)


#: ``SaasWorkloadConfig`` behind ``open_calendar_provider``.
CALENDAR_CONFIG = SaasWorkloadConfig(
    root_name="Calendars",
    leaf_kind=UnitKind.CALENDAR_EVENT,
    tables=(_CALENDAR_TABLE, _EVENT_TABLE),
    tree_factory=_build_tree,
    content=_content,
    leaf_export_name=_export_name,
    group_extras=_group_extras,
    leaf_extras=_event_extras,
    # The group_* names are GROUP_EXCHANGE's.
    object_names={
        _CALENDAR_TABLE: ("calendar_db", "group_calendar_db"),
        _EVENT_TABLE: ("calendar_event_db", "group_calendar_event_db"),
    },
)


#: Async factory over ``CALENDAR_CONFIG`` (see ``make_saas_provider``).
open_calendar_provider = make_saas_provider(CALENDAR_CONFIG, name="open_calendar_provider")
