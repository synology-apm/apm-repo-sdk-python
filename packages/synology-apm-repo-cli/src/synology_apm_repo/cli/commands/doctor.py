"""``synology-apm-repo-cli doctor <repo>`` — one-page repository diagnosis
built entirely on already-existing SDK calls. Available in both normal and
``--verbose`` mode — normal mode shows only display names plus each
workload's supported/unsupported status; ``--verbose`` additionally shows
internal ids and format metadata (see ``_CatalogReport``).
"""

from __future__ import annotations

import asyncio
from typing import NotRequired, TypedDict

import typer
from rich.console import Console

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.options import KeyOption, ProfileOption, RepoArgument
from synology_apm_repo.cli.paging import render
from synology_apm_repo.cli.repo_session import opened_repo_or_profile
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.sdk.api import Catalog, Repository, Workload
from synology_apm_repo.sdk.presentation.markup import safe

console = Console()


class _KeyReport(TypedDict):
    status: str
    #: Reported unconditionally, alongside ``status`` rather than
    #: instead of it — ``status``'s ``no_key_provided`` value covers both
    #: "confirmed encrypted, no key given yet" (``is_encrypted`` is
    #: ``True``) and the rare "encryption status itself couldn't be
    #: resolved" (``is_encrypted`` is ``None`` — see ``ARCHITECTURE.md``'s
    #: Repository Layer section); without this field there is no way to
    #: tell the two apart.
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
    ``--verbose`` only, and only ever set together. ``repo_uuid``/
    ``repo_type`` are genuinely this catalog's own (object storage: each
    repo-id has its own ``repo_info`` marker) — the same value repeated
    for every sibling of a vault, which shares one ``repo_info``."""

    display_name: str
    workload_count: int
    version_count: int
    workloads: list[_WorkloadReport]
    catalog_id: NotRequired[str]
    namespaces: NotRequired[list[str]]
    repo_uuid: NotRequired[str]
    repo_type: NotRequired[int | None]


class _DoctorReport(TypedDict):
    """``repo_root`` is ``--verbose`` only — the one remaining piece of
    internal repository-format metadata that's genuinely bucket/vault-wide, not
    part of the default catalog/workload/version view."""

    layout: str
    key: _KeyReport
    catalogs: list[_CatalogReport]
    repo_root: NotRequired[str]


def _key_report(repo: Repository) -> _KeyReport:
    # repo.key_status/.is_encrypted/.key_verification are plain, no-I/O
    # properties — Session.discover()/.open() already resolved the key
    # status up front when the repository was opened.
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
    # Concurrent, not serial -- mirrors Repository.catalogs()'s own
    # unbounded gather over this same "list of catalogs" collection
    # (a repo's own backup-source count, never item-tree scale). For a
    # vault (sibling catalogs sharing one serialized aiosqlite connection)
    # this is safe but may show little wall-clock gain; for object storage
    # (each catalog its own connection/store, genuinely non-blocking S3/
    # Azure I/O) it can scale with catalog count. asyncio.gather preserves
    # input order, so this list's shape is unchanged either way.
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
    console.print(f"[bold]key status[/bold]: {key['status']}")
    if key["status"] == "no_key_provided" and key["is_encrypted"] is None:
        console.print("  [dim]encryption status could not be determined[/dim]")
    if key["status"] == "invalid":
        console.print(f"  gcm_ok={key.get('gcm_ok')}")

    cats = report["catalogs"]
    console.print(f"\n[bold]{len(cats)} backup source(s)[/bold]")
    for cat in cats:
        # Every display_name/subtitle/namespace below is real repo
        # content, not structure -- escaped via safe() at interpolation
        # time.
        console.print(
            f"  [cyan]{safe(cat['display_name'])}[/cyan] — {cat['workload_count']} workload(s), "
            f"{cat['version_count']} version(s)"
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
