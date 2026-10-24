"""Shared ``typer`` ``Annotated`` aliases for the ``--key``/``--profile``/
``--object-db-id`` options and the ``REPO`` argument, so every command
taking one declares it the same way.
"""

from __future__ import annotations

from typing import Annotated

import typer

from synology_apm_repo.cli.strings import KEY_HELP, OBJECT_DB_ID_HELP, PROFILE_OPTION_HELP, REPO_PATH_HELP
from synology_apm_repo.sdk import KeyMaterialError, RawView, validate_key_string


def _parse_key(value: str) -> str:
    """``--key`` checked while the command line is read, so a malformed key
    is a usage error."""
    try:
        validate_key_string(value)
    except KeyMaterialError as exc:
        raise typer.BadParameter(exc.safe_message) from None
    return value


def _parse_optional_key(value: str) -> str | None:
    """``_parse_key``, but an empty value is no key."""
    return _parse_key(value) if value else None


#: No alias carries a default (Typer rejects one set both in the annotation
#: and on the parameter), so every call site writes its own ``= None``.
KeyOption = Annotated[str | None, typer.Option("--key", help=KEY_HELP, parser=_parse_optional_key, metavar="<str>")]
RequiredKeyOption = Annotated[str, typer.Option("--key", help=KEY_HELP, parser=_parse_key, metavar="<str>")]
ProfileOption = Annotated[str | None, typer.Option("--profile", help=PROFILE_OPTION_HELP)]
ObjectDbIdOption = Annotated[str | None, typer.Option("--object-db-id", help=OBJECT_DB_ID_HELP)]


def raw_view(object_db_id: str | None) -> RawView | None:
    """The ``RawView`` ``--object-db-id`` asks for, or ``None`` without it."""
    return RawView(object_db_id) if object_db_id is not None else None


RepoArgument = Annotated[str | None, typer.Argument(help=REPO_PATH_HELP)]
