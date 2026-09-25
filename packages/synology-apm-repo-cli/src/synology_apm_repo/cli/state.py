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

    #: See TRACE_HELP. Off by default — full-detail per-call tracing
    #: isn't something a normal invocation should pay for.
    trace: bool = False

    #: See QUIET_HELP. Specifically gates ``export``'s summary line and
    #: ``profile add``/``remove``'s green confirmations; ``--json``
    #: output is unaffected either way.
    quiet: bool = False

    #: See NO_INPUT_HELP. ``profile add``/``profile remove`` are the only
    #: commands that ever prompt; every other command already takes every
    #: input via flags/arguments and needs no gate here.
    no_input: bool = False
