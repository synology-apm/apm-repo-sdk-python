"""The bounded read-then-export check the ``device``/``fs``/``saas`` domains
run on a picked leaf."""

from __future__ import annotations

import tempfile
from pathlib import Path

from synology_apm_repo.sdk import ApmRepoError, ContentSource
from synology_apm_repo.sdk.api.export import run_export
from synology_apm_repo.sdk.dedup.local_file_sink import LocalFileSink

from .._context import SmokeContext

#: Bytes read for the in-memory "browse a meaningful item" check -- a
#: header-sized prefix, never the whole file/disk image.
HEADER_READ_CAP = 64 * 1024

#: Above this size the export round trip is skipped, keeping the one
#: disk-touching check fast and bounded.
EXPORT_SIZE_CAP = 4 * 1024 * 1024


async def bounded_read_and_export(
    ctx: SmokeContext,
    domain: str,
    step_prefix: str,
    content: ContentSource,
    *,
    degrade_on: tuple[type[ApmRepoError], ...],
) -> None:
    """Reads ``min(size, HEADER_READ_CAP)`` bytes in memory, then, if
    ``size <= EXPORT_SIZE_CAP``, exports to a temporary file and checks its
    size against ``content.size``; otherwise the export step is skipped."""
    size = content.size

    async def _read() -> int:
        cap = HEADER_READ_CAP if size is None else min(size, HEADER_READ_CAP)
        data = await content.read(0, cap)
        return len(data)

    read_len = await ctx.call(domain, f"{step_prefix}.read", _read, degrade_on=degrade_on)
    if read_len is None:
        return  # already recorded DEGRADED or FAILED by ctx.call

    if size is None or size > EXPORT_SIZE_CAP:
        ctx.skip(
            domain,
            f"{step_prefix}.export",
            f"content too large or of unknown size for the export smoke check (size={size})",
        )
        return

    with tempfile.TemporaryDirectory(prefix="apm-smoke-") as tmp_dir:
        dst = Path(tmp_dir) / "export.bin"

        async def _export() -> int:
            await run_export(content, LocalFileSink(dst, staged=False))
            return dst.stat().st_size

        exported_size = await ctx.call(domain, f"{step_prefix}.export", _export, degrade_on=degrade_on)
        if exported_size is not None:
            ctx.check(
                domain,
                f"{step_prefix}.export.size_matches",
                exported_size == size,
                note=f"exported={exported_size}, content.size={size}",
            )
