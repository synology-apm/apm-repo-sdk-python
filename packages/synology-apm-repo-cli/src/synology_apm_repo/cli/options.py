"""Shared ``typer`` ``Annotated`` parameter aliases for the ``--key``/
``--profile``/``--object-db-id`` options and the ``REPO`` argument
(``REPO_PATH_HELP`` form) — declared once here so every command taking
one of these shares the same declaration, and a future change (a short
alias, a different default) touches one place instead of every command
file.
"""

from __future__ import annotations

from typing import Annotated

import typer

from synology_apm_repo.cli.strings import KEY_HELP, OBJECT_DB_ID_HELP, PROFILE_OPTION_HELP, REPO_PATH_HELP

#: Every alias below carries no default of its own (Typer's ``Annotated``
#: style rejects a default set both inside the annotation and on the
#: parameter, per-command style pattern) — every call site still writes
#: its own ``= None``.
KeyOption = Annotated[str | None, typer.Option("--key", help=KEY_HELP)]
ProfileOption = Annotated[str | None, typer.Option("--profile", help=PROFILE_OPTION_HELP)]
ObjectDbIdOption = Annotated[str | None, typer.Option("--object-db-id", help=OBJECT_DB_ID_HELP)]
RepoArgument = Annotated[str | None, typer.Argument(help=REPO_PATH_HELP)]
