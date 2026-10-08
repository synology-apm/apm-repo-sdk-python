"""Shared ``typer`` ``Annotated`` aliases for the ``--key``/``--profile``/
``--object-db-id`` options and the ``REPO`` argument, so every command
taking one declares it the same way.
"""

from __future__ import annotations

from typing import Annotated

import typer

from synology_apm_repo.cli.strings import KEY_HELP, OBJECT_DB_ID_HELP, PROFILE_OPTION_HELP, REPO_PATH_HELP
from synology_apm_repo.sdk import RawView

#: No alias carries a default (Typer rejects one set both in the annotation
#: and on the parameter), so every call site writes its own ``= None``.
KeyOption = Annotated[str | None, typer.Option("--key", help=KEY_HELP)]
ProfileOption = Annotated[str | None, typer.Option("--profile", help=PROFILE_OPTION_HELP)]
ObjectDbIdOption = Annotated[str | None, typer.Option("--object-db-id", help=OBJECT_DB_ID_HELP)]


def raw_view(object_db_id: str | None) -> RawView | None:
    """The ``RawView`` ``--object-db-id`` asks for, or ``None`` without it."""
    return RawView(object_db_id) if object_db_id is not None else None


RepoArgument = Annotated[str | None, typer.Argument(help=REPO_PATH_HELP)]
