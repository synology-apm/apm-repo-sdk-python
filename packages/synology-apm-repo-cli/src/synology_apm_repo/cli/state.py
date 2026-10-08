"""Global CLI state: ``--verbose`` and ``--json`` are root-level flags
every subcommand reads, not per-command options — ``typer.Context.obj``
is how Typer threads them down.
"""

from __future__ import annotations

import dataclasses
import enum


class ProgressMode(enum.Enum):
    """The three ``--progress`` values (PROGRESS_HELP, progress_render.py)."""

    AUTO = "auto"
    ALWAYS = "always"
    NEVER = "never"


@dataclasses.dataclass(frozen=True, slots=True)
class CliState:
    #: See VERBOSE_HELP. Purely presentational: it adds internal-identifier
    #: detail and error-message tags, never refuses anything.
    verbose: bool = False

    #: See JSON_HELP.
    json: bool = False

    #: See PROGRESS_HELP / progress_render.py.
    progress: ProgressMode = ProgressMode.AUTO

    #: See TRACE_HELP.
    trace: bool = False

    #: See QUIET_HELP. Gates ``export``'s summary line and ``profile
    #: add``/``remove``'s confirmations; ``--json`` output is unaffected.
    quiet: bool = False

    #: See NO_INPUT_HELP. ``profile add``/``profile remove`` are the only
    #: commands that prompt.
    no_input: bool = False
