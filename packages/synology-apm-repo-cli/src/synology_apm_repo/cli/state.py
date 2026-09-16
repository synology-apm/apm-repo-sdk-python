"""Global CLI state: ``--verbose`` and ``--json`` are root-level flags
every subcommand reads, not per-command options — ``typer.Context.obj``
is how Typer threads them down.
"""

from __future__ import annotations

import dataclasses
import enum


class ProgressMode(enum.Enum):
    """See PROGRESS_HELP / progress_render.py — the three ``--progress``
    values, as a Typer-option-compatible enum instead of a bare ``str``
    checked by hand."""

    AUTO = "auto"
    ALWAYS = "always"
    NEVER = "never"


@dataclasses.dataclass(frozen=True)
class CliState:
    #: See VERBOSE_HELP. Purely presentational — nothing hard-gates on
    #: it; ``ls``/``tree``/``doctor`` use it to decide what extra
    #: (internal-identifier) detail to show.
    verbose: bool = False

    #: See JSON_HELP. Display text is not a stable dict key across
    #: language/labeling changes.
    json: bool = False

    #: See PROGRESS_HELP / progress_render.py.
    progress: ProgressMode = ProgressMode.AUTO

    #: See TRACE_HELP. Off by default: full-detail per-read tracing is
    #: not something a normal invocation should pay for. See
    #: ``cli/trace_render.py``.
    trace: bool = False

    #: See QUIET_HELP. Gates only a command's own decorative
    #: success-confirmation lines (``export``'s summary, ``profile
    #: add``/``remove``'s green confirmations) — never error output,
    #: never ``--json``, never a command's primary report.
    quiet: bool = False

    #: See NO_INPUT_HELP. ``profile add``/``profile remove`` are the only
    #: commands that ever prompt; every other command already takes every
    #: input via flags/arguments and needs no gate here.
    no_input: bool = False
