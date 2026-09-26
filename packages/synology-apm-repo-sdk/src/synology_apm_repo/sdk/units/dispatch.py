"""Provider dispatch. Lives in ``units``, not ``catalog``, so ``catalog`` never
has to import ``units`` and form a cycle. ``target_type in {VM,PC,PS}`` ->
``DeviceProvider``; ``FS`` -> ``FsProvider``; SaaS (``{M365,GW}``) routes through
``saas_provider_for``, which picks an application-layer provider by the
owning ``Workload``'s catalog-derived ``sub_type`` and degrades to
``RawObjectProvider`` on recognition failure.
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import Protocol

from ..catalog.version import Version
from ..catalog.workload import TargetType, Workload
from ..dedup.repository import DedupRepo
from ..errors import UnsupportedDataFormatError
from .base import ClosableUnitProvider
from .device import DeviceProvider
from .fs import FsProvider
from .saas.calendar import CalendarProvider
from .saas.composite_provider import CompositeSaasProvider
from .saas.contact import ContactProvider
from .saas.drive import DriveProvider
from .saas.mail import ArchiveMailProvider, MailProvider
from .saas.provider import SharedSaasContext, resolve_shared_saas_context
from .saas.raw_object import RawObjectProvider
from .saas.site import SiteProvider
from .saas.stream import SaasStreamCache
from .saas.teams_chat import TeamsChatProvider

_DEVICE_TARGET_TYPES = frozenset({TargetType.VM, TargetType.PC, TargetType.PS})

#: ``target_type`` values ``provider_for`` alone can build a provider for —
#: VM/PC/PS/FS only; SaaS dispatch lives in ``saas_provider_for`` (see
#: ``SUPPORTED_SAAS_SUB_TYPES``). Membership doesn't guarantee every
#: individual version resolves — a specific PC/PS version can still have
#: every disk fragment unresolvable at runtime.
SUPPORTED_TARGET_TYPES = _DEVICE_TARGET_TYPES | {TargetType.FS}


#: Factories are ``(repo, version, saas_streams, *, shared=...) ->
#: Awaitable[ClosableUnitProvider]``, not ``type``: six are factory
#: functions over the shared ``SaasWorkloadProvider`` class, one is a
#: classmethod. A plain ``Callable[[...], ...]`` alias can't express a
#: keyword-only parameter, hence the ``Protocol``.
class _ProviderFactory(Protocol):
    def __call__(
        self,
        repo: DedupRepo,
        version: Version,
        saas_streams: SaasStreamCache,
        *,
        shared: SharedSaasContext | None = None,
    ) -> Awaitable[ClosableUnitProvider]: ...


#: One entry per ``Workload.sub_type`` this SDK has an application-layer
#: provider for — a *candidate list* since a single sub_type can map to
#: more than one candidate (see ``saas_provider_for`` for when/why). Each
#: entry's ``str`` tag identifies which sub-provider a node belongs to
#: when more than one candidate succeeds.
_SAAS_SUB_TYPE_CANDIDATES: dict[str, tuple[tuple[str, _ProviderFactory], ...]] = {
    "MAIL": (("mail", MailProvider),),
    "CONTACT": (("contact", ContactProvider),),
    "CALENDAR": (("calendar", CalendarProvider),),
    "DRIVE": (("drive", DriveProvider),),
    "USER_DRIVE": (("drive", DriveProvider),),
    "SITE": (("site", SiteProvider),),
    "USER_EXCHANGE": (
        ("mail", MailProvider),
        ("contact", ContactProvider),
        ("calendar", CalendarProvider),
        # A real, separate M365-only mailbox coexisting with regular Mail
        # in the same version — a candidate like every other here: it
        # either succeeds or raises UnsupportedDataFormatError and is
        # absent from the sibling set. Not offered for GROUP_EXCHANGE,
        # which has no archive_mail_db entry.
        ("archive_mail", ArchiveMailProvider),
    ),
    # TeamsChatProvider.create, not the class itself: its construction is
    # an async classmethod, not __init__ -- the other five are async
    # factory functions with the same shape (see _ProviderFactory).
    "TEAMS": (("teams_chat", TeamsChatProvider.create),),
    "USER_CHAT": (("teams_chat", TeamsChatProvider.create),),
    # TEAM_DRIVE (GWS shared/"Team" Drive) and GROUP_EXCHANGE (M365
    # shared/group mailbox) are the group-owned counterparts of
    # USER_DRIVE/USER_EXCHANGE, using the same service-DB schema and
    # providers. GWS has no GROUP_EXCHANGE equivalent (Groups are mailing
    # lists, not mailboxes).
    "TEAM_DRIVE": (("drive", DriveProvider),),
    "GROUP_EXCHANGE": (("mail", MailProvider), ("contact", ContactProvider), ("calendar", CalendarProvider)),
}

#: ``Workload.sub_type`` values ``saas_provider_for`` can attempt a
#: provider for. Doesn't guarantee every version of a recognized sub_type
#: succeeds — same caveat as ``SUPPORTED_TARGET_TYPES``.
SUPPORTED_SAAS_SUB_TYPES = frozenset(_SAAS_SUB_TYPE_CANDIDATES)


def is_supported(workload: Workload) -> bool:
    """Whether ``workload`` has a chance at an application-layer provider
    (VM/PC/PS/FS via ``provider_for``, or a recognized SaaS ``sub_type``
    via ``saas_provider_for``) — a plain, no-I/O check. Carries the same
    caveat those two constants do: ``True`` doesn't guarantee every
    individual version resolves."""
    return workload.workload_type in SUPPORTED_TARGET_TYPES or workload.sub_type in SUPPORTED_SAAS_SUB_TYPES


async def provider_for(repo: DedupRepo, version: Version) -> ClosableUnitProvider:
    """Return the right ``ClosableUnitProvider`` for ``version``, based on
    its ``target_type`` alone (VM/PC/PS/FS only).

    Async to match ``UnitProvider``'s own construction uniformity, not
    because this dispatch itself does I/O: the ``target_type`` branch is a
    pure string comparison, and both ``DeviceProvider.create`` and
    ``FsProvider.__init__`` construct with no I/O.

    Raises:
        UnsupportedDataFormatError: ``version.target_type`` is a SaaS type
            (``M365``/``GW``) — those need the owning ``Workload`` too (for
            ``sub_type``); call ``saas_provider_for`` instead.
    """
    if version.target_type in _DEVICE_TARGET_TYPES:
        return await DeviceProvider.create(repo, version)
    if version.target_type == TargetType.FS:
        return FsProvider(repo, version)
    raise UnsupportedDataFormatError(
        f"no provider yet for target_type {version.target_type!r} — use saas_provider_for() for SaaS versions",
        ref=version.version_uid,
    )


async def saas_provider_for(
    repo: DedupRepo,
    workload: Workload,
    version: Version,
    saas_streams: SaasStreamCache,
    *,
    object_db_id: str | None = None,
) -> ClosableUnitProvider:
    """Route one SaaS ``Version`` to its application-layer provider(s), by
    the owning ``Workload.sub_type``. Tries every candidate rather than
    stopping at the first match — M365's ``USER_EXCHANGE``/
    ``GROUP_EXCHANGE`` can genuinely have Mail, Contact, and Calendar all
    present in the same version at once. Zero matches degrades to
    ``RawObjectProvider``; exactly one is returned directly; more than
    one is wrapped in ``CompositeSaasProvider``.

    ``saas_streams`` is threaded into every candidate and the fallback
    alike, so a stream this call opens is reused by any later call
    resolving the same stream. ``object_db_id`` only reaches the
    fallback ``RawObjectProvider`` construction.

    Async because each candidate genuinely reads (opens ``saas_obj``,
    resolves its object-name index) to decide whether it recognizes this
    version — except when more than one candidate is offered for this
    ``sub_type``, in which case ``resolve_shared_saas_context`` does that
    read once, up front, and every candidate reuses it.
    """
    candidates = _SAAS_SUB_TYPE_CANDIDATES.get(workload.sub_type or "", ())
    shared = await resolve_shared_saas_context(repo, version, saas_streams) if len(candidates) > 1 else None
    found: dict[str, ClosableUnitProvider] = {}
    try:
        for tag, factory in candidates:
            try:
                found[tag] = await factory(repo, version, saas_streams, shared=shared)
            except UnsupportedDataFormatError:
                continue
    except Exception:
        # An unexpected failure after at least one earlier candidate
        # already succeeded must not leak that candidate's own
        # connections.
        for already_built in found.values():
            await already_built.close()
        raise
    if len(found) == 1:
        return next(iter(found.values()))
    if len(found) > 1:
        return CompositeSaasProvider(repo, version, found)
    return await RawObjectProvider.create(repo, version, saas_streams, object_db_id=object_db_id)


async def raw_fallback_provider_for(
    repo: DedupRepo,
    version: Version,
    saas_streams: SaasStreamCache,
    *,
    object_db_id: str | None = None,
) -> RawObjectProvider:
    """The degradation axis, made directly reachable: diagnostic
    tooling, the TUI's ``d``-mode override, or ``saas_provider_for``
    itself can get a SaaS version's raw object-name-index tree directly.
    Kept separate from ``provider_for``, which stays a hard refusal for a
    bare ``Version`` with no ``Workload``.
    """
    return await RawObjectProvider.create(repo, version, saas_streams, object_db_id=object_db_id)
