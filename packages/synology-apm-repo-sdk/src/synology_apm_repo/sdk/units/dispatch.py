"""Provider dispatch. Lives in ``units``, not ``catalog``, so ``catalog`` never
has to import ``units`` and form a cycle. ``target_type in {VM,PC,PS}`` ->
``DeviceProvider``; ``FS`` -> ``FsProvider``; SaaS (GWS/M365) routes through
``saas_provider_for``, which picks an application-layer provider by the
owning ``Workload``'s catalog-derived ``sub_type`` and degrades to
``RawObjectProvider`` on recognition failure.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

from .._util.closing import close_preserving
from ..catalog.version import Version
from ..catalog.workload import DEVICE_TARGET_TYPES, SaasSubType, TargetType, Workload
from ..dedup.repository import DedupRepo
from ..errors import UnsupportedDataFormatError
from .base import ClosableUnitProvider
from .device import DeviceProvider
from .fs import FsProvider
from .saas.calendar import open_calendar_provider
from .saas.composite_provider import CompositeSaasProvider
from .saas.contact import open_contact_provider
from .saas.context import SharedSaasContext, resolve_shared_saas_context
from .saas.drive import open_drive_provider
from .saas.mail import open_archive_mail_provider, open_mail_provider
from .saas.object_name_index import DEGRADABLE_OPEN_ERRORS
from .saas.provider import SaasProviderFactory
from .saas.raw_object import RawObjectProvider
from .saas.site import open_site_provider
from .saas.stream import SaasStreamCache
from .saas.teams_chat import TeamsChatProvider

#: ``target_type`` values ``DeviceProvider`` serves; with FS, every one in
#: ``DEVICE_TARGET_TYPES``, which ``provider_for`` alone builds a provider for.
_DEVICE_PROVIDER_TYPES = DEVICE_TARGET_TYPES - {TargetType.FS}


_ProviderFactory = SaasProviderFactory[ClosableUnitProvider]


#: One entry per ``Workload.sub_type`` this SDK has an application-layer
#: provider for — a *candidate list* since a single sub_type can map to
#: more than one candidate (see ``saas_provider_for`` for when/why). Each
#: entry's ``str`` tag identifies which sub-provider a node belongs to
#: when more than one candidate succeeds.
_SAAS_SUB_TYPE_CANDIDATES: dict[str, tuple[tuple[str, _ProviderFactory], ...]] = {
    SaasSubType.MAIL: (("mail", open_mail_provider),),
    SaasSubType.CONTACT: (("contact", open_contact_provider),),
    SaasSubType.CALENDAR: (("calendar", open_calendar_provider),),
    SaasSubType.DRIVE: (("drive", open_drive_provider),),
    SaasSubType.USER_DRIVE: (("drive", open_drive_provider),),
    SaasSubType.SITE: (("site", open_site_provider),),
    SaasSubType.USER_EXCHANGE: (
        ("mail", open_mail_provider),
        ("contact", open_contact_provider),
        ("calendar", open_calendar_provider),
        # A separate M365-only archive mailbox coexisting with regular Mail
        # in the same version. Not offered for GROUP_EXCHANGE, which has no
        # archive_mail_db entry.
        ("archive_mail", open_archive_mail_provider),
    ),
    SaasSubType.TEAMS: (("teams_chat", TeamsChatProvider.create),),
    SaasSubType.USER_CHAT: (("teams_chat", TeamsChatProvider.create),),
    # TEAM_DRIVE (GWS shared/"Team" Drive) and GROUP_EXCHANGE (M365
    # shared/group mailbox) are the group-owned counterparts of
    # USER_DRIVE/USER_EXCHANGE, using the same service-DB schema and
    # providers. GWS has no GROUP_EXCHANGE equivalent (Groups are mailing
    # lists, not mailboxes).
    SaasSubType.TEAM_DRIVE: (("drive", open_drive_provider),),
    SaasSubType.GROUP_EXCHANGE: (
        ("mail", open_mail_provider),
        ("contact", open_contact_provider),
        ("calendar", open_calendar_provider),
    ),
}

#: ``Workload.sub_type`` values ``saas_provider_for`` can attempt a
#: provider for.
SUPPORTED_SAAS_SUB_TYPES = frozenset(_SAAS_SUB_TYPE_CANDIDATES)


def is_supported(workload: Workload) -> bool:
    """Whether ``workload`` has a chance at an application-layer provider
    (a ``DEVICE_TARGET_TYPES`` type via ``provider_for``, or a
    ``SUPPORTED_SAAS_SUB_TYPES`` sub_type via ``saas_provider_for``) — a
    plain, no-I/O check. ``True`` doesn't guarantee every version resolves
    (a PC/PS version can have every disk fragment unresolvable)."""
    return workload.workload_type in DEVICE_TARGET_TYPES or workload.sub_type in SUPPORTED_SAAS_SUB_TYPES


async def provider_for(repo: DedupRepo, version: Version) -> ClosableUnitProvider:
    """Return the right ``ClosableUnitProvider`` for ``version``, based on
    its ``target_type`` alone (VM/PC/PS/FS only). Does no I/O.

    Raises:
        UnsupportedDataFormatError: ``version.target_type`` is a SaaS type
            (GWS/M365) — those need the owning ``Workload`` too (for
            ``sub_type``); call ``saas_provider_for`` instead.
    """
    if version.target_type in _DEVICE_PROVIDER_TYPES:
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
) -> ClosableUnitProvider:
    """Route one SaaS ``Version`` to its application-layer provider(s), by
    the owning ``Workload.sub_type``. Tries every candidate, since M365's
    ``USER_EXCHANGE``/``GROUP_EXCHANGE`` can hold Mail, Contact and
    Calendar in one version. Zero matches degrades to
    ``RawObjectProvider``; exactly one is returned directly; more than
    one is wrapped in ``CompositeSaasProvider``.

    ``saas_streams`` is borrowed by every candidate and the fallback, so
    a stream opened here is reused by later calls.
    """
    candidates = _SAAS_SUB_TYPE_CANDIDATES.get(workload.sub_type or "", ())
    # Resolved once, for every candidate and the raw fallback alike.
    shared = await resolve_shared_saas_context(repo, version, saas_streams)
    async with _held_for_dispatch(shared):
        found: dict[str, ClosableUnitProvider] = {}
        try:
            for tag, factory in candidates:
                try:
                    found[tag] = await factory(repo, version, saas_streams, shared=shared)
                except UnsupportedDataFormatError:
                    continue
        except BaseException as exc:
            # A failure or cancel after an earlier candidate succeeded must not
            # leak that candidate's connections.
            await close_preserving(exc, [provider.close for provider in found.values()])
            raise
        if len(found) == 1:
            return next(iter(found.values()))
        if len(found) > 1:
            return CompositeSaasProvider(repo, version, found)
        return await RawObjectProvider.create(repo, version, saas_streams, shared=shared)


@contextlib.asynccontextmanager
async def _held_for_dispatch(shared: SharedSaasContext) -> AsyncIterator[None]:
    """Holds ``shared``'s index ObjectDB while candidates are tried in turn,
    so one that fails and releases its hold doesn't close it under the next
    (or the raw fallback), which would load it again. Best effort: an index
    that doesn't load is left for each candidate to degrade on."""
    index_db = shared.index_object_db
    held = False
    if index_db is not None:
        try:
            await index_db.acquire()
            held = True
        except DEGRADABLE_OPEN_ERRORS:
            pass
    try:
        yield
    finally:
        if held:
            assert index_db is not None
            await index_db.release()
