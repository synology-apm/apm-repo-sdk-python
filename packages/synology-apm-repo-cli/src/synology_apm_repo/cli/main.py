"""Entry point for the ``synology-apm-repo-cli`` command."""

from __future__ import annotations

from importlib.metadata import version as _pkg_version

import typer
from rich.console import Console

from synology_apm_repo.cli.commands import cat as cat_cmd
from synology_apm_repo.cli.commands import doctor as doctor_cmd
from synology_apm_repo.cli.commands import dump as dump_cmd
from synology_apm_repo.cli.commands import export as export_cmd
from synology_apm_repo.cli.commands import key as key_cmd
from synology_apm_repo.cli.commands import ls as ls_cmd
from synology_apm_repo.cli.commands import profile as profile_cmd
from synology_apm_repo.cli.commands import tree as tree_cmd
from synology_apm_repo.cli.commands import verify as verify_cmd
from synology_apm_repo.cli.state import CliState, ProgressMode
from synology_apm_repo.cli.strings import (
    APP_HELP,
    JSON_HELP,
    NO_INPUT_HELP,
    PROGRESS_HELP,
    QUIET_HELP,
    TRACE_HELP,
    VERBOSE_HELP,
    VERSION_HELP,
)
from synology_apm_repo.sdk.presentation.logging_setup import configure_logging

console = Console()

app = typer.Typer(
    name="synology-apm-repo-cli",
    help=APP_HELP,
    no_args_is_help=True,
    # Click's own default is ["--help"] only; -h is accepted as the other
    # half of that pair. Click inherits context_settings into every
    # subcommand's own context, so this covers `<command> -h` too.
    context_settings={"help_option_names": ["-h", "--help"]},
)


def _version_callback(value: bool) -> None:
    """``--version``'s eager callback: read from the installed
    distribution metadata rather than a second hardcoded literal that
    ``scripts/check_version_consistency.py`` would need to also watch."""
    if value:
        console.print(f"synology-apm-repo-cli {_pkg_version('synology-apm-repo-cli')}")
        raise typer.Exit()


@app.callback()
def _root(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "--verbose", help=VERBOSE_HELP),
    json_output: bool = typer.Option(False, "--json", help=JSON_HELP),
    progress: ProgressMode = typer.Option(ProgressMode.AUTO.value, "--progress", help=PROGRESS_HELP),
    trace: bool = typer.Option(False, "--trace", help=TRACE_HELP),
    quiet: bool = typer.Option(False, "--quiet", "-q", help=QUIET_HELP),
    no_input: bool = typer.Option(False, "--no-input", help=NO_INPUT_HELP),
    version: bool = typer.Option(False, "--version", callback=_version_callback, is_eager=True, help=VERSION_HELP),
) -> None:
    """Root callback: keeps every subcommand explicit rather than Typer's
    single-command collapse, and is where the global ``--verbose``/
    ``--json``/``--progress``/``--trace``/``--quiet``/``--no-input`` flags
    live — every subcommand reads them via ``ctx.obj``. ``--version`` is
    eager and exits before any of this runs."""
    ctx.obj = CliState(
        verbose=verbose,
        json=json_output,
        progress=progress,
        trace=trace,
        quiet=quiet,
        no_input=no_input,
    )


app.command("doctor")(doctor_cmd.doctor)
app.command("ls")(ls_cmd.ls)
app.command("tree")(tree_cmd.tree)
app.command("cat")(cat_cmd.cat)
app.command("export")(export_cmd.export)
app.command("key")(key_cmd.key)
app.command("verify")(verify_cmd.verify)
app.add_typer(dump_cmd.app, name="dump")
app.add_typer(profile_cmd.app, name="profile")


def main() -> None:  # pragma: no cover - real sys.argv/sys.exit; every test drives ``app`` via CliRunner instead
    # Silences dependency logging by default, or redirects it to a file via
    # SYNOLOGY_APM_REPO_LOG — shared with the TUI's own call site
    # (browser/app.py::main()).
    configure_logging()
    app()


if __name__ == "__main__":  # pragma: no cover - same reason as main() above
    main()
