"""``CalendarProvider``: M365/GWS Calendar via ``calendar_table`` +
``calendar_event_table``, built as a ``SaasWorkloadProvider`` +
``NamedGroupFlatTree`` config, wrapped in one extra synthetic level
(``tree_strategy.CategorizedGroupTree``) that splits the root into "My
Calendars" and "Other Calendars" by each calendar's own ownership (see
``_is_other_calendar`` for the ``calendar_type`` split rule).

Unlike Drive (one service DB), Calendar has **two separate service-level
DB snapshots in the same ObjectDB sequence** — ``calendar_table`` (the
calendar list) and ``calendar_event_table`` (its events), each named
independently in the object-name index; ``SaasWorkloadConfig``'s two-table
case locates and opens both independently.

- Tree: My/Other Calendars → calendar → event, grouped by ``calendar_id``
  (the "grouped flat list" strategy), via ``NamedGroupFlatTree``.
- Content: ``calendar_event_table.meta_object_id`` → a META JSON object;
  the spec's additional ``modified_date_list``/``exdate_list`` keys are
  never read here. ``build_ics`` assembles the ``.ics`` from
  ``client_metadata`` alone — each platform's own calendar API event
  resource, verbatim, for both GWS and M365.
- Detached occurrences are real data (see ``build_ics`` for how
  ``RECURRENCE-ID`` is derived); undetached occurrences are covered by
  the exported ``RRULE``, which the importing calendar app expands on
  its own.
- A calendar's own displayed name isn't always ``calendar_name``
  verbatim — ``_group_name_override`` corrects for two real cases: a
  user's own relabeling, and GWS's bare-email default for an unrenamed
  primary calendar.
"""

from __future__ import annotations

import json
from collections.abc import Callable

from ...storage.table import Column, Table
from ..base import RestorableUnit, UnitKind, mtime_attrs
from ..content.saas_artifact import LazyArtifact
from ..content.saas_calendar import M365_RECURRENCE_FREQ, build_ics
from .objectdb import read_object
from .provider import SaasWorkloadConfig, SaasWorkloadProvider, make_saas_provider, owning_account_user_info
from .tree_strategy import CategorizedGroupTree, NamedGroupFlatTree

_CALENDAR_TABLE = "calendar_table"
_EVENT_TABLE = "calendar_event_table"

_Row = dict[str, object | None]
_Key = tuple[str, ...]

#: The two synthetic top-level groups ``CategorizedGroupTree`` adds for
#: the My/Other Calendars split — internal key tokens, not the
#: displayed strings (``_CATEGORY_LABELS`` below maps these to those).
_CATEGORY_MY = "my"
_CATEGORY_OTHER = "other"
_CATEGORY_LABELS = {_CATEGORY_MY: "My Calendars", _CATEGORY_OTHER: "Other Calendars"}

_CALENDAR_COLUMNS = [
    Column("calendar_id"),
    Column("calendar_name"),
    Column("timezone", required=False),
    # See ``_is_other_calendar`` for what a present/absent value here means.
    Column("calendar_type", required=False),
    # A user's own personal relabeling of this calendar in their own
    # calendar list (Google's own "summaryOverride") -- distinct from
    # ``calendar_name`` (that calendar's own base name/"summary", which
    # for the account's primary calendar defaults to the bare account
    # email and is never renamed this way). See ``_group_name_override``.
    Column("calendar_name_override", required=False),
]

# Not every real Graph/Google Calendar event has a title — a missing
# title is real, expected data, not a corruption signal — shown
# literally, never a raw internal id (e.g. Microsoft Graph's own opaque
# base64-ish ``event_id``, meaningless to a user) standing in for a name.
_NO_TITLE_LABEL = "(no title)"
_EVENT_COLUMNS = [
    Column("event_id"),
    Column("calendar_id"),
    Column("summary"),
    Column("meta_object_id"),
    Column("event_start_time", required=False),
    Column("event_end_time", required=False),
    # A short recurrence label (see ``_recurrence_label``) is derived
    # from this JSON pattern column, real and populated on both
    # platforms — this connector normalizes GWS's own recurrence shape
    # into the same Microsoft Graph recurrence-pattern vocabulary too.
    Column("recurrence_rule", required=False),
]


def _event_display_name(row: _Row) -> str:
    """Display name for an event: ``row["summary"]`` if non-empty, else
    ``_NO_TITLE_LABEL`` — covers both a real empty-string summary and an
    unconfirmed-but-possible SQL ``NULL``."""
    summary = row.get("summary")
    return str(summary) if summary else _NO_TITLE_LABEL


def _recurrence_label(row: _Row) -> str:
    """A short display label for an event's own recurrence, parsed from
    ``recurrence_rule``'s JSON ``pattern.type`` — blank for a real
    one-off event, never the raw JSON. The label itself is derived from
    ``content/saas_calendar.py``'s own ``M365_RECURRENCE_FREQ`` (the
    same vocabulary ``.ics`` export already uses), not a second,
    independently-maintained pattern-type table -- a pattern type
    outside that vocabulary still renders as "Recurring" rather than
    nothing, since ``recurrence_rule`` being present at all already
    means the event isn't a one-off."""
    raw = row.get("recurrence_rule")
    if not raw:
        return ""
    try:
        parsed = json.loads(str(raw))
    except ValueError:
        return ""
    pattern = parsed.get("pattern") if isinstance(parsed, dict) else None
    if not isinstance(pattern, dict):
        return ""
    pattern_type = pattern.get("type")
    freq = M365_RECURRENCE_FREQ.get(str(pattern_type))
    return freq.capitalize() if freq else "Recurring"


def _event_extra_attrs(provider: SaasWorkloadProvider, row: _Row) -> dict[str, object]:
    # Row-only reshaping — doesn't need ``provider``, but the shared
    # extra_attrs callback signature always takes one, since other
    # workloads' extras (GWS Mail's label names, GWS Contact's group
    # names) read prefetched data from ``provider.extras``.
    del provider
    attrs: dict[str, object] = {"recurrence": _recurrence_label(row)}
    attrs.update(mtime_attrs(row.get("event_start_time"), "event_start"))
    attrs.update(mtime_attrs(row.get("event_end_time"), "event_end"))
    return attrs


def _group_attrs(provider: SaasWorkloadProvider, key: _Key) -> dict[str, object]:
    # A key shorter than 2 segments is the root (()) or a My/Other
    # Calendars category ((category,)) -- both containers whose own
    # children are further containers (categories, then individual
    # calendars), never a leaf. Overriding leaf_kind to CATEGORY_GROUP at
    # both levels keeps CALENDAR_EVENT's own leaves_only=True column spec
    # (correct one level deeper, inside an individual calendar) from also
    # filtering these two non-leaf levels' own children out of the file
    # table -- CATEGORY_GROUP is the shared non-leaf container kind
    # (also used by site.py's List category): Name+Created columns,
    # never leaves_only, since these levels' children being containers
    # is exactly what a folder listing should show, not filter out.
    del provider
    return {"leaf_kind": UnitKind.CATEGORY_GROUP} if len(key) < 2 else {}


def _group_name_override(owning_email: str | None, owning_name: str | None) -> Callable[[_Row], str | None]:
    """A calendar's own displayed name, in priority order: its real
    ``calendar_name_override`` when the user set one, else the backed-up
    account's own real name when this is that account's primary calendar
    (``calendar_id`` -- Google's own convention -- equals the account's
    email) rather than the bare email its ``calendar_name`` defaults to,
    else ``None`` (no override -- ``_NamedGroupTable`` falls back to
    ``calendar_name`` verbatim)."""

    def _override(row: _Row) -> str | None:
        explicit = row.get("calendar_name_override")
        if explicit:
            return str(explicit)
        if owning_email and owning_name and str(row.get("calendar_id")) == owning_email:
            return owning_name
        return None

    return _override


def _is_other_calendar(calendar_row: _Row) -> bool:
    # calendar_type=1 marks a subscribed/other calendar: 0 for every
    # calendar the account itself owns (including secondary ones), 1
    # for a subscribed public calendar (e.g. a "Holidays in ..."
    # calendar). None (missing column, or a genuinely unset value — no
    # real M365 calendar carries this distinction) is treated as "my
    # own", matching ``_CALENDAR_COLUMNS``' own ``required=False``
    # tolerance for this column.
    return calendar_row.get("calendar_type") == 1


async def _build_tree(provider: SaasWorkloadProvider) -> CategorizedGroupTree:
    # One direct scan of calendar_table computes every one of this
    # workload's calendar->category mappings up front — small and
    # bounded (a mailbox's own calendar count, never its event count).
    # Not read via ``inner.children_of(())``: that only ever exposes
    # ``calendar_id``/``calendar_name`` (children_of), never
    # ``calendar_type``.
    calendar_table = await Table.create(provider.table(_CALENDAR_TABLE), _CALENDAR_TABLE, _CALENDAR_COLUMNS)
    calendar_categories: dict[str, str] = {}
    async for row in calendar_table.select():
        calendar_id = str(row["calendar_id"])
        calendar_categories[calendar_id] = _CATEGORY_OTHER if _is_other_calendar(row) else _CATEGORY_MY

    # A separate, narrow lookup (not this scan's own rows): the account's
    # real identity lives in workload_config, not calendar_table.
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
        # event_start_time is real on both platforms and both real schema
        # docs (gws-calendar.md/m365-calendar.md) say outright it's there
        # "for Portal UI display/sort" — the one provider of the seven
        # where the real product itself already picked this sort column.
        # event_start_time is declared optional (some schema versions may
        # lack it) — tree_strategy/_base.py's _resolve_order_by() falls back to
        # "summary" (always present) if so, then SQLite's own implicit
        # ``rowid`` as the deterministic pagination tiebreaker regardless.
        order_by=["event_start_time", "summary"],
    )
    return CategorizedGroupTree(inner, categories=calendar_categories, labels=_CATEGORY_LABELS)


async def _assemble(provider: SaasWorkloadProvider, row: _Row, key: _Key) -> RestorableUnit:
    # No I/O here — the real reads happen inside the LazyArtifact's own
    # (awaited-at-most-once) build callback.
    meta_object_id = str(row["meta_object_id"])
    event_id = str(row["event_id"])

    async def _build() -> bytes:
        meta_bytes = await read_object(provider.object_db(_EVENT_TABLE), provider.dedup_file, meta_object_id)
        return build_ics(meta_bytes, event_id)

    name = _event_display_name(row)
    return RestorableUnit(
        ref=provider.ref_for(key),
        name=f"{name}.ics",
        is_leaf=True,
        kind=UnitKind.CALENDAR_EVENT,
        content=LazyArtifact(_build),
    )


#: ``SaasWorkloadConfig`` behind ``CalendarProvider`` — the two-table
#: (``calendar_table``/``calendar_event_table``) case described above.
CALENDAR_CONFIG = SaasWorkloadConfig(
    root_name="Calendars",
    leaf_kind=UnitKind.CALENDAR_EVENT,
    tables=(_CALENDAR_TABLE, _EVENT_TABLE),
    tree_factory=_build_tree,
    assemble=_assemble,
    group_attrs=_group_attrs,
    extra_attrs=_event_extra_attrs,
    # "calendar_db"/"calendar_event_db" for USER_EXCHANGE,
    # "group_calendar_db"/"group_calendar_event_db" for GROUP_EXCHANGE
    # (see units/saas/object_name_index.py).
    object_names={
        _CALENDAR_TABLE: ("calendar_db", "group_calendar_db"),
        _EVENT_TABLE: ("calendar_event_db", "group_calendar_event_db"),
    },
)


#: Constructor-style factory over ``CALENDAR_CONFIG`` — callable exactly
#: like a constructor (``await CalendarProvider(repo, version, saas_streams)``), with
#: an optional ``shared`` context passed straight through to
#: ``SaasWorkloadProvider.create`` for M365's multi-candidate
#: ``USER_EXCHANGE``/``GROUP_EXCHANGE`` dispatch.
CalendarProvider = make_saas_provider(CALENDAR_CONFIG, name="CalendarProvider")
