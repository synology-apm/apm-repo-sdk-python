"""Shared browse/read/export helper for the ``device``/``fs``/``saas``
domains -- the same bounded read-then-export shape all three need, once a
leaf has already been picked (``pick_workload``/``find_leaf`` now live in
``../.._shared_refs.py``, shared with ``cli/``/``browser/`` too).
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from synology_apm_repo.sdk import ApmRepoError, ContentSource

from .._context import SmokeContext

#: Bytes read for the in-memory "browse a meaningful item" check -- a
#: header-sized prefix, never the whole file/disk image.
HEADER_READ_CAP = 64 * 1024

#: Above this size, the real export_to() round trip is skipped rather than
#: attempted -- keeps the one disk-touching check in this tool fast and
#: bounded regardless of how large the real content is.
EXPORT_SIZE_CAP = 4 * 1024 * 1024


async def bounded_read_and_export(
    ctx: SmokeContext,
    domain: str,
    step_prefix: str,
    content: ContentSource,
    *,
    degrade_on: tuple[type[ApmRepoError], ...],
) -> None:
    """The two bounded, content-level checks every leaf item gets, both
    in-memory-safe except the export step, which is capped and cleaned up
    immediately:

    - ``read(0, min(size, HEADER_READ_CAP))`` -- pure in-memory.
    - If ``size <= EXPORT_SIZE_CAP``: a real ``export_to()`` round trip
      into a ``tempfile.TemporaryDirectory()`` (auto-cleaned on exit),
      checked against ``content.size``. Otherwise skipped with a reason --
      this is the one place this tool ever touches disk.
    """
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
            await content.export_to(dst)
            return dst.stat().st_size

        exported_size = await ctx.call(domain, f"{step_prefix}.export", _export, degrade_on=degrade_on)
        if exported_size is not None:
            ctx.check(
                domain,
                f"{step_prefix}.export.size_matches",
                exported_size == size,
                note=f"exported={exported_size}, content.size={size}",
            )
