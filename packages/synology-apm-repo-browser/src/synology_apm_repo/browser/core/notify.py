"""``Notify``: the toast ``Cmd`` every domain's ``Cmd`` union carries, which
each effect interpreter turns into ``notify()``.
"""

from __future__ import annotations

import dataclasses
from typing import Literal


@dataclasses.dataclass(frozen=True, slots=True)
class Notify:
    """``title`` is set only for a job's terminal outcome."""

    message: str
    severity: Literal["information", "warning", "error"] = "information"
    title: str | None = None
