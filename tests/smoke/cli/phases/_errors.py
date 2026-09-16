"""``errors`` domain: a handful of deliberately-bad invocations against
the real CLI, checking exit code 1 and a clean, single-line error --
never a leaked internal method name (``KeyRequiredError``/
``KeyMismatchError``'s wording) or a raw traceback. A wrong
key surfaces from ``Repository.catalogs()`` itself (each catalog's own
``DedupRepo.open()`` raises ``KeyMismatchError`` when the supplied key
fails its GCM-tag check -- see that method's own docstring), rendered by
the CLI's ``friendly_message()`` as "the key given was rejected", not a
raw traceback or the whole-repository-browsing gate's own wording.
"""

from __future__ import annotations

import base64

from ..._shared_refs import RepresentativeRef
from .._context import SmokeContext
from ._shared import profile_args

#: An obviously-synthetic, all-zero key -- syntactically valid but not
#: derived from any real sample, same shape as
#: ``sdk/phases/_catalog.py``'s own negative-key check.
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
        "errors", "errors.wrong_key", "doctor", ref.repo_path, "--key", _DUMMY_KEY, *profile_args(ref), expect_exit=1
    )
    ctx.check("errors", "errors.wrong_key.no_internal_method_leak", "set_key(" not in wrong_key_result.stderr)
    ctx.check(
        "errors",
        "errors.wrong_key.clean_error_no_traceback",
        bool(wrong_key_result.stderr.strip()) and "Traceback" not in wrong_key_result.stderr,
    )


__all__ = ["run"]
