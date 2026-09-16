"""``commands`` domain: one real subprocess pass per read-only command
against a picked, real ref -- exit code plus a ``--json`` parse-and-shape
check, not a re-derivation of the underlying data's own correctness
(that's ``sdk/``'s job). There is no ``info`` command -- it's merged into
``doctor``. Also covers the root-level, eager-exit flags
(``--version``/``-h``) and one ``verify --level full --progress always``
pass to exercise progress-during-verify wiring against a real,
real-duration operation.
"""

from __future__ import annotations

from ..._shared_refs import RepresentativeRef
from .._context import SmokeContext
from ._shared import common_args, parses_as_json


def run(ctx: SmokeContext) -> None:
    refs: list[RepresentativeRef] = ctx.data.get("refs", [])

    ctx.run("commands", "commands.version", "--version")
    ctx.run("commands", "commands.help.short", "-h")
    ctx.run("commands", "commands.help.doctor", "doctor", "-h")

    if not refs:
        ctx.skip("commands", "commands.no_refs", "no representative ref discovered by bootstrap")
        return

    for ref in refs:
        prefix = f"commands.{ref.sample_name}.{ref.type_key}"
        args = common_args(ref)

        ctx.run("commands", f"{prefix}.doctor", "doctor", ref.repo_path, *args)
        result = ctx.run("commands", f"{prefix}.doctor.json", "--json", "doctor", ref.repo_path, *args)
        ctx.check("commands", f"{prefix}.doctor.json.parses", parses_as_json(result.stdout))

        ctx.run("commands", f"{prefix}.ls", "ls", ref.ref, *args)
        result = ctx.run("commands", f"{prefix}.ls.json", "--json", "ls", ref.ref, *args)
        ctx.check("commands", f"{prefix}.ls.json.parses", parses_as_json(result.stdout))

        ctx.run("commands", f"{prefix}.tree", "tree", ref.ref, "--depth", "2", *args)
        result = ctx.run("commands", f"{prefix}.tree.json", "--json", "tree", ref.ref, "--depth", "2", *args)
        ctx.check("commands", f"{prefix}.tree.json.parses", parses_as_json(result.stdout))

        # Clamped to the leaf's own known size: requesting more than a
        # leaf actually has is a separate, real edge case (`cat --length`
        # beyond a tiny leaf's end currently surfaces as an unhandled
        # ValueError, not a clean error message) -- worth its own report,
        # not something this "did cat produce output" check should trip
        # over incidentally. `if ref.node.size:` (not `is not None`) is
        # deliberate: a real, legitimately empty (0-byte) leaf can't
        # produce output either, so it gets the same safe/skipped
        # treatment as an unknown size, not a false "produced no output"
        # failure.
        # "64" (not "4096") for size None/0 matches bootstrap's own safe probe-read cap.
        cat_length = str(min(4096, ref.node.size)) if ref.node.size else "64"
        cat_result = ctx.run("commands", f"{prefix}.cat", "cat", ref.ref, "--length", cat_length, *args)
        if ref.node.size:
            ctx.check("commands", f"{prefix}.cat.produced_output", cat_result.stdout_bytes > 0)
        else:
            ctx.skip("commands", f"{prefix}.cat.produced_output", "leaf has no known/nonzero size")

        if ref.key:
            # Only meaningful against a sample this bootstrap actually
            # opened with a real key -- an unencrypted repository's own `key`
            # semantics aren't what this step is trying to smoke-check.
            key_result = ctx.run("commands", f"{prefix}.key", "key", ref.repo_path, "--key", ref.key)
            first_line = next(iter(key_result.stdout.strip().splitlines()), "")
            ctx.check(
                "commands",
                f"{prefix}.key.verified",
                "verified" in key_result.stdout.lower(),
                note=first_line,
            )
        else:
            ctx.skip("commands", f"{prefix}.key", f"{ref.sample_name} is not encrypted")

        ctx.run("commands", f"{prefix}.verify.quick", "verify", ref.repo_path, "--level", "quick", *args)

    # One real, cancellable, real-duration verify --full pass, against
    # just the first ref -- not every sample, since this is here to
    # exercise 52fe645's progress-during-verify wiring at least once
    # against real data volume, not to re-run verify's own findings
    # correctness per sample (sdk/phases/_diagnostics.py already does
    # that in-process).
    first = refs[0]
    ctx.run(
        "commands",
        f"commands.{first.sample_name}.{first.type_key}.verify.full_progress",
        "--progress",
        "always",
        "verify",
        first.repo_path,
        "--level",
        "full",
        *common_args(first),
    )


__all__ = ["run"]
