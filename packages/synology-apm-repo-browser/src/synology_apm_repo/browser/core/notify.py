"""``Notify``: the one plain-toast ``Cmd`` every screen's own ``Cmd``
union (``AppCmd``, ``BrowseCmd``, ``UnitCmd``) carries, turned into
``self._app.notify()``/``self._screen.notify()`` by each screen's own
effect interpreter (``runtime/app_effects.py``, ``runtime/browse_effects.py``,
``runtime/unit_effects.py``). Shared here, not hand-duplicated once per
domain, so ``severity``'s own set of allowed values and ``title`` can't
drift between them.
"""

from __future__ import annotations

import dataclasses
from typing import Literal


@dataclasses.dataclass(frozen=True, slots=True)
class Notify:
    """``title`` is only ever set for a job's terminal outcome
    (``core/app/update.py``'s ``_notify_outcome``); ``core/browse/
    update.py``/``core/unit/update.py`` never set it, and their own
    effect interpreters pass it straight through to ``Widget.notify()``
    regardless."""

    message: str
    severity: Literal["information", "warning", "error"] = "information"
    title: str | None = None
