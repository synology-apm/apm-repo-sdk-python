"""``synology-apm-repo-cli doctor <repo>`` — one-page repository diagnosis.
The default view shows display names and each workload's supported status;
``--verbose`` adds internal ids and format metadata.
"""

from __future__ import annotations

import asyncio
from typing import NotRequired, TypedDict

import typer

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.consoles import console
from synology_apm_repo.cli.options import KeyOption, ProfileOption, RepoArgument
from synology_apm_repo.cli.paging import render
from synology_apm_repo.cli.repo_session import opened_repo_or_profile
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.sdk import Catalog, KeyStatus, Repository, Workload
from synology_apm_repo.sdk.presentation import pluralize, safe


class _KeyReport(TypedDict):
    status: str
    #: Always reported: it tells ``no_key_provided`` for an encrypted
    #: repository (``True``) from one whose encryption status couldn't be
    #: resolved (``None``).
    is_encrypted: bool | None
    gcm_ok: NotRequired[bool]


class _WorkloadReport(TypedDict):
    """``workload_id``/``workload_type``/``sub_type`` are ``--verbose``
    only."""

    display_name: str
    subtitle: str | None
    supported: bool
    workload_id: NotRequired[int]
    workload_type: NotRequired[str]
    sub_type: NotRequired[str | None]


class _CatalogReport(TypedDict):
    """``catalog_id``/``namespaces``/``repo_uuid``/``repo_type`` are
    ``--verbose`` only, and set together. ``repo_uuid``/``repo_type`` come
    from the catalog's ``repo_info``: per repo-id on object storage, the
    same for every catalog of a vault."""

    display_name: str
    workload_count: int
    version_count: int
    workloads: list[_WorkloadReport]
    catalog_id: NotRequired[str]
    namespaces: NotRequired[list[str]]
    repo_uuid: NotRequired[str]
    repo_type: NotRequired[int | None]


class _DoctorReport(TypedDict):
    """``repo_root`` is ``--verbose`` only."""

    layout: str
    key: _KeyReport
    catalogs: list[_CatalogReport]
    repo_root: NotRequired[str]


def _key_report(repo: Repository) -> _KeyReport:
    # No I/O: opening the repository already resolved its key status.
    report: _KeyReport = {"status": repo.key_status.value, "is_encrypted": repo.is_encrypted}
    verification = repo.key_verification
    if verification is not None:
        report["gcm_ok"] = verification.gcm_ok
    return report


def _workload_report(repo: Repository, wl: Workload, verbose: bool) -> _WorkloadReport:
    report: _WorkloadReport = {
        "display_name": wl.display_name,
        "subtitle": wl.subtitle,
        "supported": repo.workload_is_supported(wl),
    }
    if verbose:
        report["workload_id"] = wl.workload_id
        report["workload_type"] = wl.workload_type
        report["sub_type"] = wl.sub_type
    return report


async def _catalog_report(repo: Repository, catalog: Catalog, verbose: bool) -> _CatalogReport:
    wls = await catalog.workloads()
    cat = catalog.connection
    report: _CatalogReport = {
        "display_name": cat.display_name,
        "workload_count": cat.workload_count,
        "version_count": cat.version_count,
        "workloads": [_workload_report(repo, wl, verbose) for wl in wls],
    }
    if verbose:
        report["catalog_id"] = str(catalog.catalog_id)
        report["namespaces"] = list(cat.namespaces)
        parsed = catalog.info
        report["repo_uuid"] = parsed.uuid
        report["repo_type"] = parsed.repo_type
    return report


async def _build_report(repository: Repository, state: CliState) -> _DoctorReport:
    catalogs = await repository.catalogs()
    # asyncio.gather preserves input order.
    catalog_reports = await asyncio.gather(
        *(_catalog_report(repository, catalog, state.verbose) for catalog in catalogs)
    )
    report: _DoctorReport = {
        "layout": repository.layout.kind.value,
        "key": _key_report(repository),
        "catalogs": list(catalog_reports),
    }
    if state.verbose:
        report["repo_root"] = repository.layout.repo_root or "."
    return report


def _render_human(report: _DoctorReport, verbose: bool) -> None:
    console.print(f"[bold]layout[/bold]: {report['layout']}")
    if "repo_root" in report:
        console.print(f"[bold]repo_root[/bold]: {report['repo_root']}")
    key = report["key"]
    console.print(f"[bold]key status[/bold]: {KeyStatus(key['status']).label}")
    if key["status"] == "no_key_provided" and key["is_encrypted"] is None:
        console.print("  [dim]encryption status could not be determined[/dim]")
    if key["status"] == "invalid":
        console.print(f"  gcm_ok={key.get('gcm_ok')}")

    cats = report["catalogs"]
    console.print(f"\n[bold]{len(cats)} {pluralize(len(cats), 'backup source')}[/bold]")
    for cat in cats:
        # display_name/subtitle/namespace are repository content: safe() them.
        console.print(
            f"  [cyan]{safe(cat['display_name'])}[/cyan] — "
            f"{cat['workload_count']} {pluralize(cat['workload_count'], 'workload')}, "
            f"{cat['version_count']} {pluralize(cat['version_count'], 'version')}"
        )
        if verbose:
            console.print(f"    [dim]catalog_id={cat['catalog_id']}[/dim]")
            console.print(f"    [dim]repo_uuid={cat['repo_uuid']}[/dim]")
            console.print(f"    [dim]repo_type={cat['repo_type']}[/dim]")
            if cat["namespaces"]:
                console.print(f"    [dim]namespaces={', '.join(safe(ns) for ns in cat['namespaces'])}[/dim]")
        for wl in cat["workloads"]:
            marker = "" if wl["supported"] else " [red](unsupported)[/red]"
            subtitle = f" · {safe(wl['subtitle'])}" if wl.get("subtitle") else ""
            console.print(f"    - {safe(wl['display_name'])}{subtitle}{marker}")
            if verbose:
                console.print(
                    f"      [dim]workload_id={wl['workload_id']}, workload_type={wl['workload_type']}, "
                    f"sub_type={wl['sub_type']}[/dim]"
                )


@typer_async
async def doctor(
    ctx: typer.Context,
    repo: RepoArgument = None,
    key: KeyOption = None,
    profile: ProfileOption = None,
) -> None:
    """One-page diagnosis: layout, key status, and every
    catalog/workload/version this repository has. A name shown here containing
    a literal ``/`` needs percent-encoding (``/`` → ``%2F``) before it's
    usable in another command's REF — see `synology-apm-repo-cli ls --help`."""
    state: CliState = ctx.obj
    async with opened_repo_or_profile(repo, key, profile=profile, state=state) as repository:
        report = await _build_report(repository, state)

    render(console, state, json=report, human=lambda: _render_human(report, state.verbose))
