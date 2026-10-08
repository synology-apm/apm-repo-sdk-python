"""``export_lifecycle`` domain: a real file export to a temp dir (no
leftover ``.part``, a non-empty final file), plus a real, timed ``SIGINT``
mid-export.

A first Ctrl-C during ``export`` is a handled cancellation: it reports the
cancel, deletes the ``.part`` file (no ``--keep-partial``), never writes the
destination, and exits ``ExitCode.CANCELLED`` (130).
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ..._shared_refs import RepresentativeRef
from .._context import SmokeContext
from ._shared import key_args

#: Below this size an export can finish before a SIGINT sent after
#: _CANCEL_AFTER seconds lands, so the cancellation check is skipped.
_CANCEL_MIN_SIZE = 20 * 1024 * 1024
_CANCEL_AFTER = 0.2


def run(ctx: SmokeContext) -> None:
    refs: list[RepresentativeRef] = ctx.data.get("refs", [])
    if not refs:
        ctx.skip("export_lifecycle", "export_lifecycle.no_refs", "no representative ref discovered by bootstrap")
        return

    ref = refs[0]
    args = key_args(ref)

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
            *key_args(candidate),
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
