"""``synology-apm-repo-cli profile add|list|show|remove`` — manage named
S3/Azure/SMB connection profiles (``sdk.profiles``), so reconnecting to a
known bucket/container/share doesn't mean retyping every field — and
pasting the secret — from scratch each time.

Secrets (S3's access/secret key, Azure's credential, SMB's password) are
always collected interactively — via a hidden prompt (``typer.prompt``
with ``hide_input=True``) by default, or read one-per-line off stdin under
``--no-input`` (see ``_read_secret``) — never a
``--access-key``/``--secret-key``/``--password``-style flag, which would
land in shell history and process listings. A blank answer means "use the
ambient credential chain" (``aioboto3``'s default chain / Azure's
``DefaultAzureCredential``) for S3/Azure, or attempt an anonymous/guest
session for SMB.
"""

from __future__ import annotations

import sys

import typer
from rich.console import Console

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.errors import fail, fail_from_apm_error, unwrap
from synology_apm_repo.cli.paging import render
from synology_apm_repo.cli.progress_render import build_progress_meter, finish_live_progress
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
from synology_apm_repo.cli.trace_render import build_trace_callback
from synology_apm_repo.sdk.api import Session
from synology_apm_repo.sdk.errors import ApmRepoError, ProfileNotFoundError
from synology_apm_repo.sdk.profiles import (
    BackendKind,
    Profile,
    config_from_fields,
    delete_profile,
    get_profile,
    list_profiles_full,
    save_profile,
    store_from_config,
)
from synology_apm_repo.sdk.storage import ObjectStore

app = typer.Typer(help="Manage saved S3/Azure/SMB connection profiles.")

console = Console()


def _require_piped_stdin() -> None:
    """``--no-input``'s guard before reading a secret off stdin: a real
    terminal means nobody is going to pipe an answer, so failing fast here
    beats blocking forever on ``sys.stdin.readline()``."""
    if sys.stdin.isatty():
        fail("--no-input needs secrets piped on stdin, but stdin is a terminal")


def _read_secret(label: str, *, no_input: bool) -> str:
    """One secret, collected the way ``--no-input`` says to: a hidden,
    interactive prompt by default, or one line off stdin (blank line ==
    "" == "use the ambient credential chain", matching the interactive
    prompt's own blank-answer meaning) when set. ``label`` is the same
    prompt text either mode would show, so a script piping input in the
    prompts' own order (access_key then secret_key; credential for Azure;
    password for SMB) can be built by reading this function's callers top
    to bottom."""
    if not no_input:
        return str(typer.prompt(label, default="", hide_input=True, show_default=False))
    _require_piped_stdin()
    return sys.stdin.readline().rstrip("\n")


def _collect_fields(
    base: dict[str, str | bool],
    optional: dict[str, str | None],
    secrets: list[tuple[str, str]],
    *,
    no_input: bool,
) -> dict[str, str | bool]:
    """The shape ``_collect_s3_fields``/``_collect_azure_fields``/
    ``_collect_smb_fields`` each repeat: start from ``base``'s required
    fields, add each of ``optional``'s fields only when it's truthy, then
    read one or more secrets in the order a script piping input on stdin
    needs to supply them (``secrets`` is ``[(field_name, prompt_label),
    ...]``, in that order)."""
    fields: dict[str, str | bool] = {**base, **{name: value for name, value in optional.items() if value}}
    for name, label in secrets:
        fields[name] = _read_secret(label, no_input=no_input)
    return fields


def _collect_s3_fields(
    bucket: str, endpoint: str | None, region: str | None, *, verify_tls: bool, no_input: bool
) -> dict[str, str | bool]:
    return _collect_fields(
        {"bucket": bucket, "verify_tls": verify_tls},
        {"endpoint": endpoint, "region": region},
        [
            ("access_key", "Access key (blank for ambient credential chain)"),
            ("secret_key", "Secret key (blank for ambient credential chain)"),
        ],
        no_input=no_input,
    )


def _collect_azure_fields(
    container: str, account_url: str | None, *, verify_tls: bool, no_input: bool
) -> dict[str, str | bool]:
    return _collect_fields(
        {"container": container, "verify_tls": verify_tls},
        {"account_url": account_url},
        [("credential", "Credential — account key or SAS token (blank for ambient credential chain)")],
        no_input=no_input,
    )


def _collect_smb_fields(
    server: str, share: str, port: int, username: str | None, *, no_input: bool
) -> dict[str, str | bool]:
    return _collect_fields(
        {"server": server, "share": share, "port": str(port)},
        {"username": username},
        [("password", "Password (blank for an anonymous/guest session)")],
        no_input=no_input,
    )


async def _store_from_fields(kind: BackendKind, fields: dict[str, str | bool]) -> ObjectStore:
    """Builds an ``ObjectStore`` from not-yet-saved fields, for ``add``'s
    pre-persist connectivity check, via ``store_from_config`` — the same
    translation ``build_store`` applies to an already-saved profile,
    applied here before either exists."""
    # verify_tls is the one bool field; the rest (access_key/secret_key/
    # credential) are the string secrets client_kwargs_with_secrets reads.
    secret_source = {k: v for k, v in fields.items() if isinstance(v, str)}
    config = config_from_fields(kind, fields)
    return await store_from_config(kind, config, secret_source)


async def _verify_connectivity(state: CliState, backend: BackendKind, fields: dict[str, str | bool]) -> int:
    """``add``'s pre-persist connectivity check: build a store from
    not-yet-saved ``fields``, open a ``Session`` against it, and return
    how many repositories were found. Never returns on failure — every
    stage (store construction, session open) ``fail()``s the command
    instead of raising back to the caller."""
    try:
        store = await _store_from_fields(backend, fields)
    except Exception as exc:  # AzureStore validates account_url/credential shape
        # synchronously and can raise (e.g. ValueError); S3Store defers this to
        # first read/listdir.
        fail(f"connectivity check failed: {exc}", cause=exc)
    session = Session()
    meter = build_progress_meter(state)
    trace = build_trace_callback(state)
    try:
        repos = await session.open_remote(store, progress=meter.update, trace=trace)
    except ApmRepoError as exc:
        fail_from_apm_error(exc, state, prefix="connectivity check failed: ")
    finally:
        # Covers the success path (this line only clears something once,
        # even though the except branch above already called it once for
        # its own timing needs) and, unlike that branch, also a
        # non-ApmRepoError exception this function doesn't otherwise catch.
        finish_live_progress(state)
        await session.close()
    return len(repos)


@typer_async
async def add(
    ctx: typer.Context,
    name: str = typer.Argument(..., help=PROFILE_NAME_HELP),
    backend: BackendKind = typer.Option(..., "--backend", help=PROFILE_BACKEND_HELP),
    bucket: str | None = typer.Option(None, "--bucket", help=PROFILE_BUCKET_HELP),
    container: str | None = typer.Option(None, "--container", help=PROFILE_CONTAINER_HELP),
    endpoint: str | None = typer.Option(None, "--endpoint", help=PROFILE_ENDPOINT_HELP),
    region: str | None = typer.Option(None, "--region", help=PROFILE_REGION_HELP),
    account_url: str | None = typer.Option(None, "--account-url", help=PROFILE_ACCOUNT_URL_HELP),
    server: str | None = typer.Option(None, "--server", help=PROFILE_SERVER_HELP),
    share: str | None = typer.Option(None, "--share", help=PROFILE_SHARE_HELP),
    port: int = typer.Option(445, "--port", help=PROFILE_PORT_HELP),
    username: str | None = typer.Option(None, "--username", help=PROFILE_USERNAME_HELP),
    verify: bool = typer.Option(True, "--verify/--no-verify", help=PROFILE_VERIFY_HELP),
    verify_tls: bool = typer.Option(True, "--verify-tls/--no-verify-tls", help=PROFILE_VERIFY_TLS_HELP),
    force: bool = typer.Option(False, "--force", help=PROFILE_FORCE_HELP),
) -> None:
    """Save a new connection profile under NAME, prompting for its
    secret(s). Refuses to overwrite an existing profile of the same name
    unless --force is given."""
    state: CliState = ctx.obj
    if backend is BackendKind.S3 and not bucket:
        fail("--bucket is required for --backend s3")
    if backend is BackendKind.AZURE and not container:
        fail("--container is required for --backend azure")
    if backend is BackendKind.SMB and not (server and share):
        fail("--server and --share are required for --backend smb")

    if not force:
        try:
            await get_profile(name)
        except ProfileNotFoundError:
            pass
        else:
            fail(f"profile {name!r} already exists — use --force to overwrite")

    if backend is BackendKind.S3:
        assert bucket is not None
        fields = _collect_s3_fields(bucket, endpoint, region, verify_tls=verify_tls, no_input=state.no_input)
    elif backend is BackendKind.AZURE:
        assert container is not None
        fields = _collect_azure_fields(container, account_url, verify_tls=verify_tls, no_input=state.no_input)
    else:
        assert server is not None
        assert share is not None
        fields = _collect_smb_fields(server, share, port, username, no_input=state.no_input)

    if verify:
        repo_count = await _verify_connectivity(state, backend, fields)
        if repo_count == 0:
            fail(
                "connectivity check found no repositories — check the bucket/container and "
                "credentials, or pass --no-verify to save anyway"
            )
        if not state.quiet:
            console.print(f"[green]verified[/green] — found {repo_count} repository(s)")

    await save_profile(name, backend, fields)
    if not state.quiet:
        console.print(f"[green]saved[/green] profile {name!r} ({backend.value})")


@typer_async
async def list_profiles_command(ctx: typer.Context) -> None:
    """List every saved profile's name and backend, plus (under
    --verbose) the same per-backend fields ``show`` prints for one
    profile. Never touches the keyring."""
    state: CliState = ctx.obj
    # One read either way: list_profiles_full() already returns every
    # saved profile in full, so --verbose needs no per-name get_profile().
    profiles = await list_profiles_full()
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
    # display_fields keeps both renderers below backend-agnostic. verify_tls
    # is S3/Azure-only, so it's read via getattr and omitted otherwise.
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
async def show(ctx: typer.Context, name: str = typer.Argument(..., help=PROFILE_NAME_HELP)) -> None:
    """Print profile NAME's saved (non-secret) fields. Never touches the
    keyring, so secrets structurally can't leak into this output."""
    state: CliState = ctx.obj
    profile = await unwrap(get_profile(name), verbose=state.verbose)

    render(console, state, json=_build_report(profile), human=lambda: _render_human(profile))


@typer_async
async def remove(
    ctx: typer.Context,
    name: str = typer.Argument(..., help=PROFILE_NAME_HELP),
    force: bool = typer.Option(False, "--force", help=PROFILE_REMOVE_FORCE_HELP),
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
