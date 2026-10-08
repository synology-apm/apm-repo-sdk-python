"""``synology-apm-repo-cli profile add|list|show|remove`` — manage saved
S3/Azure/SMB connection profiles (``sdk.profiles``).

Secrets are read from a hidden prompt, or one per line from stdin under
``--no-input``, never from a flag, which would land in shell history and
process listings. A blank secret means the ambient credential chain for
S3/Azure, or an anonymous/guest session for SMB.
"""

from __future__ import annotations

import sys
from typing import Annotated

import typer

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.consoles import console
from synology_apm_repo.cli.errors import fail, unwrap
from synology_apm_repo.cli.paging import render
from synology_apm_repo.cli.repo_session import cli_session
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import (
    PROFILE_ACCOUNT_URL_HELP,
    PROFILE_BACKEND_HELP,
    PROFILE_BUCKET_HELP,
    PROFILE_CONTAINER_HELP,
    PROFILE_ENDPOINT_HELP,
    PROFILE_FORCE_HELP,
    PROFILE_NAME_HELP,
    PROFILE_PORT_HELP,
    PROFILE_REGION_HELP,
    PROFILE_REMOVE_FORCE_HELP,
    PROFILE_SERVER_HELP,
    PROFILE_SHARE_HELP,
    PROFILE_USERNAME_HELP,
    PROFILE_VERIFY_HELP,
    PROFILE_VERIFY_TLS_HELP,
)
from synology_apm_repo.sdk.presentation import pluralize
from synology_apm_repo.sdk.profiles import (
    DEFAULT_SMB_PORT,
    BackendKind,
    Profile,
    ProfileFieldError,
    ProfileNotFoundError,
    config_from_fields,
    delete_profile,
    form_fields_for,
    get_profile,
    list_profiles,
    save_profile,
    store_from_fields,
)

app = typer.Typer(help="Manage saved S3/Azure/SMB connection profiles.")


def _require_piped_stdin() -> None:
    """``--no-input``'s guard before reading a secret off stdin: fail rather
    than block on a terminal nobody pipes into."""
    if sys.stdin.isatty():
        fail("--no-input needs secrets piped on stdin, but stdin is a terminal")


def _read_secret(label: str, *, no_input: bool) -> str:
    """One secret: a hidden interactive prompt (``label``), or under
    ``--no-input`` one line off stdin, where a blank line means the same as
    a blank prompt answer."""
    if not no_input:
        return str(typer.prompt(label, default="", hide_input=True, show_default=False))
    _require_piped_stdin()
    return sys.stdin.readline().rstrip("\n")


def _collect_fields(kind: BackendKind, given: dict[str, object], *, no_input: bool) -> dict[str, str | bool]:
    """``kind``'s profile fields: each non-secret one from ``given`` (the
    command's flags; an unset or blank one is left out), each secret read
    in ``form_fields_for``'s order, which is also the order a script piping
    them on stdin must supply them in."""
    fields: dict[str, str | bool] = {}
    for spec in form_fields_for(kind):
        if spec.secret:
            fields[spec.name] = _read_secret(spec.prompt, no_input=no_input)
        elif (value := given.get(spec.name)) is not None and value != "":
            fields[spec.name] = value if isinstance(value, bool) else str(value)
    return fields


async def _verify_connectivity(state: CliState, backend: BackendKind, fields: dict[str, str | bool]) -> int:
    """``add``'s check before saving: how many repositories a store built
    from ``fields`` holds. A ``ProfileFieldError`` or ``ApmRepoError``
    ``fail()``s the command."""
    try:
        store = await store_from_fields(backend, fields)
    except ProfileFieldError as exc:
        fail(f"connectivity check failed: {exc}", cause=exc)
    async with cli_session(state, error_prefix=lambda: "connectivity check failed: ") as cli:
        repos = await cli.session.open(store, progress=cli.progress, trace=cli.trace)
    return len(repos)


@typer_async
async def add(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help=PROFILE_NAME_HELP)],
    backend: Annotated[BackendKind, typer.Option("--backend", help=PROFILE_BACKEND_HELP)],
    bucket: Annotated[str | None, typer.Option("--bucket", help=PROFILE_BUCKET_HELP)] = None,
    container: Annotated[str | None, typer.Option("--container", help=PROFILE_CONTAINER_HELP)] = None,
    endpoint: Annotated[str | None, typer.Option("--endpoint", help=PROFILE_ENDPOINT_HELP)] = None,
    region: Annotated[str | None, typer.Option("--region", help=PROFILE_REGION_HELP)] = None,
    account_url: Annotated[str | None, typer.Option("--account-url", help=PROFILE_ACCOUNT_URL_HELP)] = None,
    server: Annotated[str | None, typer.Option("--server", help=PROFILE_SERVER_HELP)] = None,
    share: Annotated[str | None, typer.Option("--share", help=PROFILE_SHARE_HELP)] = None,
    port: Annotated[int, typer.Option("--port", help=PROFILE_PORT_HELP)] = DEFAULT_SMB_PORT,
    username: Annotated[str | None, typer.Option("--username", help=PROFILE_USERNAME_HELP)] = None,
    verify: Annotated[bool, typer.Option("--verify/--no-verify", help=PROFILE_VERIFY_HELP)] = True,
    verify_tls: Annotated[bool, typer.Option("--verify-tls/--no-verify-tls", help=PROFILE_VERIFY_TLS_HELP)] = True,
    force: Annotated[bool, typer.Option("--force", help=PROFILE_FORCE_HELP)] = False,
) -> None:
    """Save a new connection profile under NAME, prompting for its
    secret(s). Refuses to overwrite an existing profile of the same name
    unless --force is given."""
    state: CliState = ctx.obj
    given: dict[str, object] = {
        "bucket": bucket,
        "container": container,
        "endpoint": endpoint,
        "region": region,
        "account_url": account_url,
        "server": server,
        "share": share,
        "port": port,
        "username": username,
        "verify_tls": verify_tls,
    }
    # Checked before any secret prompt, so a missing flag never costs a typed secret.
    try:
        config_from_fields(backend, {k: v for k, v in given.items() if isinstance(v, str | bool | int)})
    except ProfileFieldError as exc:
        fail(f"--{exc.field.replace('_', '-')} is required for --backend {backend}")

    if not force:
        try:
            await get_profile(name)
        except ProfileNotFoundError:
            pass
        else:
            fail(f"profile {name!r} already exists — use --force to overwrite")

    fields = _collect_fields(backend, given, no_input=state.no_input)

    if verify:
        repo_count = await _verify_connectivity(state, backend, fields)
        if repo_count == 0:
            fail(
                "connectivity check found no repositories — check the bucket/container and "
                "credentials, or pass --no-verify to save anyway"
            )
        if not state.quiet:
            console.print(
                f"[green]verified[/green] — found {repo_count} {pluralize(repo_count, 'repository', 'repositories')}"
            )

    await save_profile(name, backend, fields)
    if not state.quiet:
        console.print(f"[green]saved[/green] profile {name!r} ({backend.value})")


@typer_async
async def list_profiles_command(ctx: typer.Context) -> None:
    """List every saved profile's name and backend, plus (under
    --verbose) the same per-backend fields ``show`` prints for one
    profile. Never touches the keyring."""
    state: CliState = ctx.obj
    profiles = await list_profiles()
    if not state.verbose:
        render(
            console,
            state,
            json=[{"name": p.name, "kind": p.kind.value} for p in profiles],
            human=lambda: _render_summary_lines(profiles),
        )
        return

    render(console, state, json=[_build_report(p) for p in profiles], human=lambda: _render_verbose_list(profiles))


def _render_summary_lines(profiles: list[Profile]) -> None:
    if not profiles:
        console.print("[dim](no saved profiles)[/dim]")
        return
    for profile in profiles:
        console.print(f"{profile.name}  [dim]{profile.kind.value}[/dim]")


def _render_verbose_list(profiles: list[Profile]) -> None:
    if not profiles:
        console.print("[dim](no saved profiles)[/dim]")
        return
    for i, profile in enumerate(profiles):
        if i:
            console.print()
        _render_human(profile)


def _build_report(profile: Profile) -> dict[str, object]:
    # verify_tls is S3/Azure-only, so it's read via getattr.
    report: dict[str, object] = {"name": profile.name, "kind": profile.kind.value}
    verify_tls = getattr(profile.config, "verify_tls", None)
    if verify_tls is not None:
        report["verify_tls"] = verify_tls
    report.update(profile.config.display_fields)
    return report


def _render_human(profile: Profile) -> None:
    console.print(f"{'name':<11}: {profile.name}")
    console.print(f"{'backend':<11}: {profile.kind.value}")
    for key, value in profile.config.display_fields.items():
        console.print(f"{key:<11}: {value or '(default)'}")
    verify_tls = getattr(profile.config, "verify_tls", None)
    if verify_tls is not None:
        console.print(f"{'verify_tls':<11}: {verify_tls}")


@typer_async
async def show(ctx: typer.Context, name: Annotated[str, typer.Argument(help=PROFILE_NAME_HELP)]) -> None:
    """Print profile NAME's saved (non-secret) fields. Never touches the
    keyring, so secrets structurally can't leak into this output."""
    state: CliState = ctx.obj
    profile = await unwrap(get_profile(name), verbose=state.verbose)

    render(console, state, json=_build_report(profile), human=lambda: _render_human(profile))


@typer_async
async def remove(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help=PROFILE_NAME_HELP)],
    force: Annotated[bool, typer.Option("--force", help=PROFILE_REMOVE_FORCE_HELP)] = False,
) -> None:
    """Delete profile NAME (config and keyring secrets alike). Asks for
    confirmation unless --force is given."""
    state: CliState = ctx.obj
    if not force:
        if state.no_input:
            fail("--no-input requires --force to remove without confirmation")
        if not typer.confirm(f"Remove profile {name!r}?", default=False):
            console.print("[dim]aborted[/dim]")
            return
    await unwrap(delete_profile(name), verbose=state.verbose)
    if not state.quiet:
        console.print(f"[green]removed[/green] profile {name!r}")


app.command("add")(add)
app.command("list")(list_profiles_command)
app.command("show")(show)
app.command("remove")(remove)
