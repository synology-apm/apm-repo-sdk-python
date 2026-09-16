"""``CalendarProvider``: M365/GWS Calendar via ``calendar_table`` +
``calendar_event_table``, built as a ``SaasWorkloadProvider`` +
``NamedGroupFlatTree`` config.

Unlike Drive (one service DB), Calendar has **two separate service-level
DB snapshots in the same ObjectDB sequence** — ``calendar_table`` (the
calendar list) and ``calendar_event_table`` (its events), each named
independently in the object-name index; ``SaasWorkloadConfig``'s two-table
case locates and opens both independently.

- Tree: calendar → event, grouped by ``calendar_id`` (the "grouped flat
  list" strategy), via ``NamedGroupFlatTree``.
- Content: ``calendar_event_table.meta_object_id`` → a META JSON object;
  the spec's additional ``modified_date_list``/``exdate_list`` keys are
  never read here. ``build_ics`` assembles the ``.ics`` from
  ``client_metadata`` alone — each platform's own calendar API event
  resource, verbatim, for both GWS and M365.
- Detached occurrences are real data (see ``build_ics`` for how
  ``RECURRENCE-ID`` is derived); undetached occurrences are covered by
  the exported ``RRULE``, which the importing calendar app expands on
  its own.
"""

from __future__ import annotations

from ...storage.table import Column
from ..base import RestorableUnit, UnitKind
from ..content.saas_artifact import LazyArtifact
from ..content.saas_calendar import build_ics
from .objectdb import read_object
from .provider import SaasWorkloadConfig, SaasWorkloadProvider, make_saas_provider
from .tree_strategy import NamedGroupFlatTree

_CALENDAR_TABLE = "calendar_table"
_EVENT_TABLE = "calendar_event_table"

_Row = dict[str, object | None]
_Key = tuple[str, ...]

_CALENDAR_COLUMNS = [Column("calendar_id"), Column("calendar_name"), Column("timezone", required=False)]

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
]


def _event_display_name(row: _Row) -> str:
    """Display name for an event: ``row["summary"]`` if non-empty, else
    ``_NO_TITLE_LABEL`` — covers both a real empty-string summary and an
    unconfirmed-but-possible SQL ``NULL``."""
    summary = row.get("summary")
    return str(summary) if summary else _NO_TITLE_LABEL


async def _build_tree(provider: SaasWorkloadProvider) -> NamedGroupFlatTree:
    # No I/O of its own — ``async`` only because ``SaasWorkloadConfig``'s one
    # ``tree_factory`` field type has to cover Drive's, which does read.
    return NamedGroupFlatTree(
        provider,
        group_table=_CALENDAR_TABLE,
        group_columns=_CALENDAR_COLUMNS,
        group_id_column="calendar_id",
        group_name_column="calendar_name",
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
        # lack it) — tree_strategy.py's _resolve_order_by() falls back to
        # "summary" (always present) if so, then SQLite's own implicit
        # ``rowid`` as the deterministic pagination tiebreaker regardless.
        order_by=["event_start_time", "summary"],
    )


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
#: (``calendar_table``/``calendar_event_table``) case described in this
#: module's own docstring.
CALENDAR_CONFIG = SaasWorkloadConfig(
    root_name="Calendars",
    leaf_kind=UnitKind.CALENDAR_EVENT,
    tables=(_CALENDAR_TABLE, _EVENT_TABLE),
    tree_factory=_build_tree,
    assemble=_assemble,
    # "calendar_db"/"calendar_event_db" for USER_EXCHANGE,
    # "group_calendar_db"/"group_calendar_event_db" for GROUP_EXCHANGE
    # (see units/saas/object_name_index.py).
    object_names={
        _CALENDAR_TABLE: ("calendar_db", "group_calendar_db"),
        _EVENT_TABLE: ("calendar_event_db", "group_calendar_event_db"),
    },
)


#: Constructor-style factory over ``CALENDAR_CONFIG`` — see
#: ``make_saas_provider``'s own docstring for what "constructor-style
#: factory" and ``shared`` mean.
CalendarProvider = make_saas_provider(CALENDAR_CONFIG, name="CalendarProvider")
