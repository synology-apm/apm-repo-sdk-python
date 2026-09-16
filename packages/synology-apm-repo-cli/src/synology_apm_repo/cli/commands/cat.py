"""``synology-apm-repo-cli cat <ref>`` — write one unit's content to stdout. Content
goes to stdout; every diagnostic/error goes to stderr, so
``synology-apm-repo-cli cat <ref> > out.bin`` is never polluted.
"""

from __future__ import annotations

import sys

import typer

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.browse import opened_repo, parse_ref_argument, resolve_restorable
from synology_apm_repo.cli.options import KeyOption, ObjectDbIdOption, ProfileOption
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import CAT_LENGTH_HELP, CAT_OFFSET_HELP, REF_HELP_SINGLE_ITEM


@typer_async
async def cat(
    ctx: typer.Context,
    ref: str = typer.Argument(..., help=REF_HELP_SINGLE_ITEM),
    key: KeyOption = None,
    offset: int = typer.Option(0, "--offset", min=0, help=CAT_OFFSET_HELP),
    length: int | None = typer.Option(None, "--length", min=0, help=CAT_LENGTH_HELP),
    object_db_id: ObjectDbIdOption = None,
    profile: ProfileOption = None,
) -> None:
    """Write REF's content to stdout."""
    state: CliState = ctx.obj
    parsed = parse_ref_argument(ref)
    async with opened_repo(parsed.fs_path, key, profile=profile, state=state) as repo:
        resolved = await resolve_restorable(
            repo,
            parsed.node_ref,
            ref=ref,
            hint="use `ls`/`tree` to see what's inside it",
            object_db_id=object_db_id,
        )
        content = resolved.open()
        data = await content.read(offset, length)
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()
