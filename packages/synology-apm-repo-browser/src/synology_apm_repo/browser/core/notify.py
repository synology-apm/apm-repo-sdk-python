"""``Notify``: the one plain-toast ``Cmd`` every screen's ``Cmd`` union
carries, turned into ``notify()`` by each screen's own effect
interpreter. Shared here so ``severity``/``title`` can't drift between
domains.
"""

from __future__ import annotations

import dataclasses
from typing import Literal


@dataclasses.dataclass(frozen=True, slots=True)
class Notify:
    """``title`` is only set for a job's terminal outcome; other domains
    never set it, and their effect interpreters pass it through regardless."""

    message: str
    severity: Literal["information", "warning", "error"] = "information"
    title: str | None = None
