"""``global_flags`` domain: the root-level flags every subcommand shares
-- ``--verbose``'s soft "show more" gating (no subcommand hard-refuses
without it; it's only a field-visibility difference), ``--quiet`` never
suppressing error output, ``--progress``'s NDJSON stream landing only on
stderr, and ``--trace`` emitting at least one line to stderr without
polluting stdout. This is CLI-plumbing territory ``sdk/`` smoke has no
equivalent of at all.
"""

from __future__ import annotations

from ..._shared_refs import RepresentativeRef
from .._context import SmokeContext
from ._shared import common_args, parses_as_json


def run(ctx: SmokeContext) -> None:
    refs: list[RepresentativeRef] = ctx.data.get("refs", [])
    if not refs:
        ctx.skip("global_flags", "global_flags.no_refs", "no representative ref discovered by bootstrap")
        return

    ref = refs[0]
    repo_path = ref.repo_path
    args = common_args(ref)

    # --verbose: a field only present with --verbose, in both human and
    # --json output for doctor (83404f6's json/human-parity fix).
    plain = ctx.run("global_flags", "global_flags.verbose.doctor_json_plain", "--json", "doctor", repo_path, *args)
    verbose = ctx.run(
        "global_flags", "global_flags.verbose.doctor_json_verbose", "--verbose", "--json", "doctor", repo_path, *args
    )
    ctx.check(
        "global_flags",
        "global_flags.verbose.gates_repo_uuid",
        '"repo_uuid"' not in plain.stdout and '"repo_uuid"' in verbose.stdout,
    )

    # --quiet: gates only decorative confirmation lines, never error
    # output -- checked here against a deliberately-bad ref rather than
    # depending on _export_lifecycle.py's/_profile.py's own success-path
    # confirmation lines.
    bad_ref = f"{repo_path}#cat:not-a-real-id"
    quiet_error = ctx.run(
        "global_flags", "global_flags.quiet.errors_still_show", "--quiet", "ls", bad_ref, expect_exit=1
    )
    ctx.check("global_flags", "global_flags.quiet.error_not_suppressed", "error" in quiet_error.stderr.lower())

    # --progress: NDJSON, stderr-only -- but only under --json too;
    # --progress always alone still renders a human progress bar
    # (progress_render.py's build_progress_meter gates NDJSON on
    # state.json, not state.progress). verify --level full is real,
    # cancellable, scales with data size -- exactly the kind of operation
    # worth checking the stream's shape against, per 52fe645.
    progress_result = ctx.run(
        "global_flags",
        f"global_flags.progress.{ref.sample_name}.{ref.type_key}",
        "--json",
        "--progress",
        "always",
        "verify",
        repo_path,
        "--level",
        "full",
        *args,
    )
    stderr_lines = [line for line in progress_result.stderr.splitlines() if line.strip()]
    progress_lines = [line for line in stderr_lines if parses_as_json(line)]
    ctx.check("global_flags", "global_flags.progress.emitted_ndjson_lines", len(progress_lines) > 0)

    # --trace: at least one line to stderr, --json stdout stays pure JSON.
    traced = ctx.run("global_flags", "global_flags.trace.doctor_json", "--trace", "--json", "doctor", repo_path, *args)
    ctx.check("global_flags", "global_flags.trace.stderr_has_lines", bool(traced.stderr.strip()))
    ctx.check("global_flags", "global_flags.trace.stdout_still_parses", parses_as_json(traced.stdout))


__all__ = ["run"]
