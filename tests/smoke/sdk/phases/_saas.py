"""``saas`` domain: GW/M365 workloads, looped over whatever
``Workload.sub_type``s are actually present (``MAIL``/``CONTACT``/
``CALENDAR``/``DRIVE``/``SITE``/``TEAMS``/...) rather than a fixed list --
absent sub_types are skipped by name instead of assumed present.

Any leaf kind counts as "meaningful" here (unlike device/fs): a SaaS
provider's leaves are always one of ``MAIL``/``CONTACT``/
``CALENDAR_EVENT``/``DRIVE_ITEM``/``SITE_ITEM``/``FILE`` (Teams-chat
messages assemble to an HTML ``FILE``, per ``ARCHITECTURE.md``'s Content
Layer section) or the ``RAW_OBJECT`` degrade fallback -- the
sample/sub_type grouping already tells the report which content family a
step covers, so this phase doesn't also need to guess an exact
sub_type-to-kind mapping.
"""

from __future__ import annotations

from synology_apm_repo.sdk import (
    ChunkCompactedError,
    DataCorruptError,
    Node,
    NotFoundError,
    RestorableUnit,
    TargetType,
    UnitKind,
    UnitProvider,
    UnsupportedDataFormatError,
    Version,
    Workload,
)
from synology_apm_repo.sdk.identifiers import CatalogId

from ..._shared_refs import close_if_closable, pick_workload_with_retry, prefer_adversarial_name
from .._context import RepoInfo, SmokeContext
from ._shared import bounded_read_and_export

_SAAS_TYPES = (TargetType.GW, TargetType.M365)
_ANY_LEAF = frozenset(UnitKind)
_DEGRADE_ON = (NotFoundError, DataCorruptError, ChunkCompactedError, UnsupportedDataFormatError)


async def run_for_repo(ctx: SmokeContext, ri: RepoInfo) -> None:
    workloads: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]] = ctx.data.get("workloads", [])
    own = [
        (r, c, w, v)
        for r, c, w, v in workloads
        if r is ri and w.workload_type in _SAAS_TYPES and w.sub_type is not None
    ]
    sub_types = sorted({w.sub_type for _r, _c, w, _v in own if w.sub_type is not None})
    if not sub_types:
        # Purely per-repo judgment: this sample alone lacking a GW/M365
        # workload with a known sub_type is reported here, not deferred
        # to a once-per-run aggregate.
        ctx.skip(
            "saas",
            f"saas.{ri.sample_name}.workload_present",
            f"no GW/M365 workload with a known sub_type in {ri.sample_name}",
        )
        return

    for sub_type in sub_types:
        entries = [t for t in own if t[2].sub_type == sub_type]
        step_prefix = f"saas.{ri.sample_name}.{sub_type}"
        if not ri.readable:
            ctx.skip(
                "saas", f"{step_prefix}.browse", f"{ri.sample_name} is encrypted, key_status={ri.key_status.value}"
            )
            continue

        picked, truncated = await pick_workload_with_retry(entries, _ANY_LEAF, prefer=prefer_adversarial_name)
        if picked is None:
            if truncated:
                ctx.check(
                    "saas",
                    f"{step_prefix}.browse",
                    False,
                    note="search bound exhausted on every candidate, no matching leaf found",
                )
            else:
                ctx.skip("saas", f"{step_prefix}.browse", "no working version found across any candidate")
            continue
        _ri, _workload, _version, picked_provider, picked_leaf = picked

        # The winning candidate is this call's own to close once done --
        # pick_workload_with_retry only closes rejected ones (see its own
        # docstring). A SaaS-rich sample can have a dozen-plus distinct
        # sub_types in one turn, each its own provider materializing its
        # own SqliteSource, so leaving these open across iterations adds
        # up fast.
        try:

            async def _get_provider(provider: UnitProvider = picked_provider) -> UnitProvider:
                return provider

            provider = await ctx.call("saas", f"{step_prefix}.provider", _get_provider, degrade_on=_DEGRADE_ON)
            if provider is None:
                continue

            async def _get_unit(provider: UnitProvider = provider, leaf: Node = picked_leaf) -> RestorableUnit:
                return await provider.unit(leaf)

            unit = await ctx.call("saas", f"{step_prefix}.unit", _get_unit, degrade_on=_DEGRADE_ON)
            if unit is None:
                continue
            content = unit.open()
            await bounded_read_and_export(ctx, "saas", step_prefix, content, degrade_on=_DEGRADE_ON)
        finally:
            await close_if_closable(picked_provider)
