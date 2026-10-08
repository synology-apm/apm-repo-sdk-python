"""``errors`` domain: deliberately bad invocations (a missing path, a wrong
key), checking exit code 1 and a clean error on stderr -- no traceback, no
leaked SDK method name.
"""

from __future__ import annotations

import base64

from ..._shared_refs import RepresentativeRef
from .._context import SmokeContext

#: A well-formed, all-zero key no real sample uses.
_DUMMY_KEY = "DUMMYKEYID12@" + base64.b64encode(bytes(32)).decode()


def run(ctx: SmokeContext) -> None:
    bad_path_result = ctx.run(
        "errors", "errors.bad_path", "doctor", "/nonexistent/path/that/should/not/exist", expect_exit=1
    )
    ctx.check("errors", "errors.bad_path.reports_error", bool(bad_path_result.stderr.strip()))

    refs: list[RepresentativeRef] = ctx.data.get("refs", [])
    encrypted = [r for r in refs if r.key]
    if not encrypted:
        ctx.skip("errors", "errors.wrong_key", "no encrypted sample configured")
        return

    ref = encrypted[0]
    wrong_key_result = ctx.run(
        "errors", "errors.wrong_key", "doctor", ref.repo_path, "--key", _DUMMY_KEY, expect_exit=1
    )
    ctx.check("errors", "errors.wrong_key.no_internal_method_leak", "set_key(" not in wrong_key_result.stderr)
    ctx.check(
        "errors",
        "errors.wrong_key.clean_error_no_traceback",
        bool(wrong_key_result.stderr.strip()) and "Traceback" not in wrong_key_result.stderr,
    )


__all__ = ["run"]
