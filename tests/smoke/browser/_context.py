"""Run state for one ``python -m tests.smoke.browser`` invocation; phases
record in-process ``Pilot`` steps through ``ctx.call``.
"""

from __future__ import annotations

from dataclasses import dataclass

from .._context import CallContext

#: One ``phases/_<domain>.py`` each.
DOMAINS = (
    "navigate",
    "diagnostics_and_verbose",
    "export_worklist",
    "export_folder",
    "hex_preview",
    "key_dialog",
    "remote_connect",
    "help_screen",
)


@dataclass
class SmokeContext(CallContext):
    DOMAINS = DOMAINS
    TITLE = "Browser"
