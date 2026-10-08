"""``synology-apm-repo-cli cat <ref>`` — write one unit's content to stdout;
diagnostics and errors go to stderr. Output is streamed in chunks: a unit
can be a multi-GB disk image, and one bulk ``read()`` would buffer it all
and can exceed the SDK's single-read ceiling. ``stream()`` takes no offset,
so ``--offset``/``--length`` use a chunked ``read()`` loop instead.
"""

from __future__ import annotations

import sys
from typing import Annotated

import typer

from synology_apm_repo.cli.asyncio_support import typer_async
from synology_apm_repo.cli.browse import parse_ref_argument, resolve_restorable
from synology_apm_repo.cli.options import KeyOption, ObjectDbIdOption, ProfileOption
from synology_apm_repo.cli.repo_session import opened_repo
from synology_apm_repo.cli.state import CliState
from synology_apm_repo.cli.strings import CAT_LENGTH_HELP, CAT_OFFSET_HELP, REF_HELP_SINGLE_ITEM

#: Chunk size for the ``--offset``/``--length`` read loop, a CLI-local choice.
_OFFSET_READ_BLOCK = 8 << 20  # 8 MiB


@typer_async
async def cat(
    ctx: typer.Context,
    ref: Annotated[str, typer.Argument(help=REF_HELP_SINGLE_ITEM)],
    key: KeyOption = None,
    offset: Annotated[int, typer.Option("--offset", min=0, help=CAT_OFFSET_HELP)] = 0,
    length: Annotated[int | None, typer.Option("--length", min=0, help=CAT_LENGTH_HELP)] = None,
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
        content = resolved.content
        if offset == 0 and length is None:
            async for _pos, chunk in content.stream():
                sys.stdout.buffer.write(chunk)
        else:
            pos = offset
            remaining = length
            while remaining is None or remaining > 0:
                want = _OFFSET_READ_BLOCK if remaining is None else min(_OFFSET_READ_BLOCK, remaining)
                chunk = await content.read(pos, want)
                if not chunk:
                    break
                sys.stdout.buffer.write(chunk)
                pos += len(chunk)
                if remaining is not None:
                    remaining -= len(chunk)
        sys.stdout.buffer.flush()
