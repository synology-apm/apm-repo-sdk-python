"""``commands`` domain: each read-only command against every picked ref --
exit code plus a ``--json`` parse check, not data correctness (``sdk/``'s
job) -- and the eager-exit ``--version``/``-h``. ``verify`` runs at
``--level quick`` only.
"""

from __future__ import annotations

from ..._shared_refs import RepresentativeRef
from .._context import VERIFY_COMPLETED, SmokeContext
from ._shared import key_args, parses_as_json


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
        args = key_args(ref)

        ctx.run("commands", f"{prefix}.doctor", "doctor", ref.repo_path, *args)
        result = ctx.run("commands", f"{prefix}.doctor.json", "--json", "doctor", ref.repo_path, *args)
        ctx.check("commands", f"{prefix}.doctor.json.parses", parses_as_json(result.stdout))

        ctx.run("commands", f"{prefix}.ls", "ls", ref.ref, *args)
        result = ctx.run("commands", f"{prefix}.ls.json", "--json", "ls", ref.ref, *args)
        ctx.check("commands", f"{prefix}.ls.json.parses", parses_as_json(result.stdout))

        ctx.run("commands", f"{prefix}.tree", "tree", ref.ref, "--depth", "2", *args)
        result = ctx.run("commands", f"{prefix}.tree.json", "--json", "tree", ref.ref, "--depth", "2", *args)
        ctx.check("commands", f"{prefix}.tree.json.parses", parses_as_json(result.stdout))

        # Clamped to the leaf's size: reading past a leaf's end is its own
        # edge case, not this check's. An unknown or 0-byte size reads
        # bootstrap's 64-byte probe and skips the output check.
        cat_length = str(min(4096, ref.node.size)) if ref.node.size else "64"
        cat_result = ctx.run("commands", f"{prefix}.cat", "cat", ref.ref, "--length", cat_length, *args)
        if ref.node.size:
            ctx.check("commands", f"{prefix}.cat.produced_output", cat_result.stdout_bytes > 0)
        else:
            ctx.skip("commands", f"{prefix}.cat.produced_output", "leaf has no known/nonzero size")

        if ref.key:
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

        ctx.run(
            "commands",
            f"{prefix}.verify.quick",
            "verify",
            ref.repo_path,
            "--level",
            "quick",
            *args,
            expect_exit=VERIFY_COMPLETED,
        )


__all__ = ["run"]
