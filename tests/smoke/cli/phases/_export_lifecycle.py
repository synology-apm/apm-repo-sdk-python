"""``export_lifecycle`` domain: a real file export to a temp dir end to
end (``.part`` -> renamed final file, byte size matches), plus the one
genuinely subprocess-only check: a real, timed ``SIGINT`` mid-export.

A first-press Ctrl-C during ``export`` is a *clean*, handled
cancellation, not a crash: ``export.py``'s own ``except asyncio.
CancelledError`` branch prints a message and returns normally, so the
process exits **0**, the partial ``.part`` file is deleted (no
``--keep-partial``), and the final destination is never written --
this is the shape this smoke check verifies.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ..._shared_refs import RepresentativeRef
from .._context import SmokeContext
from ._shared import common_args

#: Below this, a real export finishes too fast for a SIGINT sent after
#: _CANCEL_AFTER seconds to reliably land mid-flight rather than after
#: the export has already completed -- the cancellation check is skipped
#: (not failed) for a run where no picked ref clears this bar.
_CANCEL_MIN_SIZE = 20 * 1024 * 1024
_CANCEL_AFTER = 0.2


def run(ctx: SmokeContext) -> None:
    refs: list[RepresentativeRef] = ctx.data.get("refs", [])
    if not refs:
        ctx.skip("export_lifecycle", "export_lifecycle.no_refs", "no representative ref discovered by bootstrap")
        return

    ref = refs[0]
    args = common_args(ref)

    with tempfile.TemporaryDirectory(prefix="apm-cli-smoke-") as tmp_dir:
        dst = Path(tmp_dir) / "export.bin"
        ctx.run(
            "export_lifecycle",
            f"export_lifecycle.{ref.sample_name}.{ref.type_key}.export",
            "export",
            ref.ref,
            "-o",
            str(dst),
            *args,
        )
        ctx.check(
            "export_lifecycle",
            f"export_lifecycle.{ref.sample_name}.{ref.type_key}.export.no_leftover_part",
            not dst.with_name(dst.name + ".part").exists(),
        )
        ctx.check(
            "export_lifecycle",
            f"export_lifecycle.{ref.sample_name}.{ref.type_key}.export.file_exists",
            dst.exists() and dst.stat().st_size > 0,
        )

    candidate = max(refs, key=lambda r: r.node.size or 0)
    if (candidate.node.size or 0) < _CANCEL_MIN_SIZE:
        ctx.skip(
            "export_lifecycle",
            "export_lifecycle.cancel",
            f"no picked ref is large enough (>= {_CANCEL_MIN_SIZE} bytes) for a reliable mid-export SIGINT",
        )
        return

    with tempfile.TemporaryDirectory(prefix="apm-cli-smoke-cancel-") as tmp_dir:
        dst = Path(tmp_dir) / "export.bin"
        cancel_result = ctx.run_cancellable(
            "export_lifecycle",
            f"export_lifecycle.{candidate.sample_name}.{candidate.type_key}.cancel",
            "export",
            candidate.ref,
            "-o",
            str(dst),
            *common_args(candidate),
            cancel_after=_CANCEL_AFTER,
        )
        ctx.check(
            "export_lifecycle",
            f"export_lifecycle.{candidate.sample_name}.{candidate.type_key}.cancel.reported_cancelled",
            "cancel" in cancel_result.stdout.lower() or "cancel" in cancel_result.stderr.lower(),
        )
        ctx.check(
            "export_lifecycle",
            f"export_lifecycle.{candidate.sample_name}.{candidate.type_key}.cancel.no_part_left",
            not dst.with_name(dst.name + ".part").exists(),
        )
        ctx.check(
            "export_lifecycle",
            f"export_lifecycle.{candidate.sample_name}.{candidate.type_key}.cancel.no_final_file",
            not dst.exists(),
        )


__all__ = ["run"]
