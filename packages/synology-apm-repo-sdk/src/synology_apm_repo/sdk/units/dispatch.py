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
#: VM/PC/PS/FS only. SaaS dispatch needs the owning ``Workload`` (for
#: ``sub_type``), so it lives in the separate ``saas_provider_for`` below;
#: see ``SUPPORTED_SAAS_SUB_TYPES`` for that axis's equivalent. Membership
#: here doesn't guarantee every individual version resolves — a specific
#: PC/PS version can still have every disk fragment unresolvable at
#: runtime (``device_pcps.PcpsDiskTree``'s own diagnostic-node path).
SUPPORTED_TARGET_TYPES = _DEVICE_TARGET_TYPES | {TargetType.FS}


#: Factories are ``(repo, version, saas_streams, *, shared=...) ->
#: Awaitable[ClosableUnitProvider]``, not ``type``: six are factory
#: functions over the shared ``SaasWorkloadProvider`` class, one is a
#: classmethod. Every candidate is awaitable — building any SaaS provider
#: opens the version's ``saas_obj`` (via ``saas_streams``, reused across
#: every version of the same stream rather than opened fresh) and resolves
#: its service DB via the connector's own object-name index (never a
#: scan), both real I/O — unless ``shared`` is given, in which case that
#: work was already done once by ``resolve_shared_saas_context`` (see
#: ``saas_provider_for``). A plain ``Callable[[...], ...]`` alias can't
#: express a keyword-only parameter, hence the ``Protocol``.
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
        # in the same version — a candidate like every other here, not
        # special-cased: it either
        # succeeds (a real archive_mail_db object-name index entry resolved) or
        # raises UnsupportedDataFormatError and is simply absent from the
        # sibling set, exactly like any other candidate that doesn't
        # recognize this version. Not offered for GROUP_EXCHANGE —
        # GROUP_EXCHANGE's additional_meta never has an archive_mail_db
        # entry.
        ("archive_mail", ArchiveMailProvider),
    ),
    # ``TeamsChatProvider.create``, not the class itself: it is the only
    # candidate that is a real class, and its construction is an ``async``
    # classmethod rather than ``__init__`` — the other five are async
    # factory functions with the same ``(repo, version, *, shared=...)
    # -> await provider`` shape (see ``_ProviderFactory`` above).
    "TEAMS": (("teams_chat", TeamsChatProvider.create),),
    "USER_CHAT": (("teams_chat", TeamsChatProvider.create),),
    # TEAM_DRIVE (GWS shared/"Team" Drive) and GROUP_EXCHANGE (M365
    # shared/group mailbox) are the group-owned counterparts of
    # USER_DRIVE/USER_EXCHANGE above, and use the same service-DB schema
    # and the same providers. GWS has no GROUP_EXCHANGE equivalent —
    # Google Workspace Groups are mailing lists, not mailboxes with their
    # own Calendar/Contacts the way an M365 shared/group mailbox is;
    # TEAM_DRIVE is GWS's only group-owned counterpart to a per-user
    # workload.
    "TEAM_DRIVE": (("drive", DriveProvider),),
    "GROUP_EXCHANGE": (("mail", MailProvider), ("contact", ContactProvider), ("calendar", CalendarProvider)),
}

#: ``Workload.sub_type`` values ``saas_provider_for`` can attempt an
#: application-layer provider for. This does **not** guarantee every
#: individual version of a workload with a recognized sub_type succeeds
#: (a specific version can still degrade to raw at runtime) — same
#: caveat ``SUPPORTED_TARGET_TYPES`` already carries for PC/PS.
SUPPORTED_SAAS_SUB_TYPES = frozenset(_SAAS_SUB_TYPE_CANDIDATES)


def is_supported(workload: Workload) -> bool:
    """Whether ``workload`` has a chance at an application-layer provider
    (VM/PC/PS/FS via ``provider_for``, or a recognized SaaS ``sub_type`` via
    ``saas_provider_for``) — a plain, no-I/O check over
    ``SUPPORTED_TARGET_TYPES``/``SUPPORTED_SAAS_SUB_TYPES``, for callers
    (e.g. the CLI's ``doctor`` command, via
    ``Repository.workload_is_supported``) that want to report "not yet
    supported" without constructing a provider. Carries the same caveat
    those two constants do: ``True`` doesn't guarantee every individual
    version actually resolves — a specific version can still fail
    construction or degrade to a raw fallback at runtime."""
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
    the owning ``Workload.sub_type``. Tries every candidate in
    ``_SAAS_SUB_TYPE_CANDIDATES`` rather than stopping at the first match
    — M365's ``USER_EXCHANGE``/``GROUP_EXCHANGE`` can genuinely have Mail,
    Contact, and Calendar all present in the same version at once. Zero
    matches degrades to ``RawObjectProvider``; exactly one is returned
    directly; more than one is wrapped in ``CompositeSaasProvider`` so
    every match stays reachable as a sibling top-level group.

    M365's ``USER_EXCHANGE``/``GROUP_EXCHANGE`` bundle Mail/Contact/
    Calendar under one sub_type; GWS's own account instead has
    independent workloads per app type (``"MAIL"``/``"CONTACT"``/
    ``"CALENDAR"``), so its candidate lists carry only one entry each.

    ``saas_streams`` — the caller's shared ``SaasStreamCache`` (built
    against this same ``repo``, never one built independently — see
    ``api.repository._OpenCatalog``) — is threaded into every candidate
    and the fallback alike, so a stream this call opens is reused by
    every *other* version of the same stream a later call resolves too,
    not just this call's own sibling candidates.

    ``object_db_id`` only reaches the fallback ``RawObjectProvider``
    construction — the application-layer candidates above resolve their
    own service DB internally.

    Async because each candidate ``factory(...)`` genuinely reads (opens
    ``saas_obj``, resolves its object-name index) to decide whether it
    recognizes this version at all — except when more than one candidate
    is offered for this ``sub_type`` (``USER_EXCHANGE``/``GROUP_EXCHANGE``
    today, per ``_SAAS_SUB_TYPE_CANDIDATES``), in which case that read
    happens exactly once, up front, via ``resolve_shared_saas_context``,
    and every candidate reuses the result instead of each re-resolving
    the same ``(repo, version)``'s ``saas_obj``/object-name index for itself. A
    single-candidate ``sub_type`` gets ``shared=None`` here, since there's
    only one candidate to share the read with.
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
        # An unexpected failure (not the routine "doesn't recognize this
        # version" UnsupportedDataFormatError) after at least one earlier
        # candidate already succeeded must not leak that candidate's own
        # connections — nothing else tracks a provider built here until
        # this function returns it.
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
    tooling, the TUI's ``d``-mode override, or ``saas_provider_for`` itself
    (when no application-layer provider recognizes a version) can get a
    SaaS version's raw object-name-index tree directly. Kept separate from
    ``provider_for``'s dispatch — that one stays a hard refusal for a bare
    ``Version`` with no ``Workload``, keeping the diagnostic-only distinction
    visible at the type level.

    Async because ``RawObjectProvider.create`` is — it opens the version's
    ``saas_obj`` (via ``saas_streams``) and resolves its object-name index.
    """
    return await RawObjectProvider.create(repo, version, saas_streams, object_db_id=object_db_id)
