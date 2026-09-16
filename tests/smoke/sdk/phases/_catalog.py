"""``catalog`` domain: correctness checks over what each repository's own
catalog reports -- discovery and catalog/workload/version enumeration
itself is driven per-repository by ``__main__.py``'s own loop, not this
module. Every key/encryption check also lives here, in one place, rather
than split across domains: this is where the key material discovery
already resolved is in scope.
"""

from __future__ import annotations

import base64

from synology_apm_repo.sdk import KeyStatus, Version, Workload
from synology_apm_repo.sdk.identifiers import CatalogId

from .._context import RepoInfo, SmokeContext

#: An obviously-synthetic, all-zero key -- syntactically valid
#: ("<12-char userKeyID>@<base64 of 32 raw bytes>") but not derived from
#: any real sample, used only to exercise set_key()'s rejection path.
_DUMMY_KEY = "DUMMYKEYID12@" + base64.b64encode(bytes(32)).decode()

#: Wall-clock budget for one sample's own catalog enumeration
#: (``__main__.py``'s per-repo loop, ``ctx.data["bootstrap_elapsed"]``) --
#: calibrated against the slowest real sample this project has measured
#: (`nas-Mia_test1`: 228+221 workloads, ~9300 versions, over a real SMB
#: mount: 14.1s measured), with real headroom above that for ordinary
#: run-to-run/network variance, not tuned tight to that one number. Not
#: about small samples at all -- catches a genuine performance
#: regression (a query that stops batching, say) before it's only
#: noticed by someone waiting on a real, large repository.
_ENUMERATION_BUDGET_SECONDS = 60.0


async def run_for_repo(ctx: SmokeContext, ri: RepoInfo) -> None:
    workloads: list[tuple[RepoInfo, CatalogId, Workload, list[Version]]] = ctx.data.get("workloads", [])
    own_workloads = [(r, c, w, v) for r, c, w, v in workloads if r is ri]

    elapsed = ctx.data.get("bootstrap_elapsed", {}).get(ri.sample_name)
    if elapsed is not None:
        ctx.check(
            "catalog",
            f"catalog.enumeration_budget[{ri.sample_name}]",
            elapsed <= _ENUMERATION_BUDGET_SECONDS,
            note=f"{elapsed:.1f}s for {len(own_workloads)} workload(s)",
        )

    for _ri, _catalog, workload, versions in own_workloads:
        step = f"catalog.workload_is_supported[{ri.sample_name}.{workload.workload_id}]"

        async def _check(ri: RepoInfo = ri, workload: Workload = workload) -> bool:
            return ri.repo.workload_is_supported(workload)

        await ctx.call("catalog", step, _check)
        # An empty version list is a legitimate, documented outcome for
        # some samples (ps-sample-1's fids are all absent from the current
        # generation -- see its own comment in your smoke_samples.toml)
        # -- recorded as a plain fact via ctx.check, never asserted to be
        # non-empty.
        ctx.check(
            "catalog",
            f"catalog.versions_non_negative[{ri.sample_name}.{workload.workload_id}]",
            len(versions) >= 0,
            note=f"{len(versions)} browsable version(s)",
        )

    if not ri.repo.is_encrypted:
        # Purely per-repo judgment: this sample alone not being encrypted
        # is reported here, not deferred to a once-per-run aggregate.
        ctx.skip("catalog", f"catalog.encryption[{ri.sample_name}]", f"{ri.sample_name} is not encrypted")
        return

    ctx.check(
        "catalog",
        f"catalog.key_status[{ri.sample_name}]",
        ri.key_status in (KeyStatus.VERIFIED, KeyStatus.NO_KEY_PROVIDED, KeyStatus.INVALID),
        note=f"key_status={ri.key_status.value}",
    )

    if not ri.readable:
        ctx.skip(
            "catalog",
            f"catalog.set_key_wrong_key.precondition[{ri.sample_name}]",
            f"{ri.sample_name} has no working key configured, key_status={ri.key_status.value}",
        )
        return

    # The negative path, for every encrypted+keyed repository independently:
    # a deliberately wrong (but syntactically valid) key must land on
    # INVALID, not silently keep the previously-verified key's status.
    # Always this repo's own catalog turn first (__main__.py's per-repo
    # loop runs "catalog" before any other domain for a given repo), so
    # the correct key is always restored below before any later domain
    # reads this same shared Repository instance.
    async def _try_wrong_key(ri: RepoInfo = ri) -> KeyStatus:
        await ri.repo.set_key(_DUMMY_KEY)
        return ri.repo.key_status

    result = await ctx.call("catalog", f"catalog.set_key_wrong_key[{ri.sample_name}]", _try_wrong_key)
    if result is not None:
        ctx.check(
            "catalog",
            f"catalog.set_key_wrong_key.invalid[{ri.sample_name}]",
            result is KeyStatus.INVALID,
            note=f"key_status={result.value}",
        )

    async def _restore_key(ri: RepoInfo = ri) -> KeyStatus:
        await ri.repo.set_key(ri.key)
        return ri.repo.key_status

    restored = await ctx.call("catalog", f"catalog.set_key_wrong_key.restore[{ri.sample_name}]", _restore_key)
    if restored is not None:
        ctx.check(
            "catalog",
            f"catalog.set_key_wrong_key.restore.verified[{ri.sample_name}]",
            restored is KeyStatus.VERIFIED,
            note=f"key_status={restored.value}",
        )
