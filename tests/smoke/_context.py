"""Generic step/result bookkeeping shared by ``sdk/``, ``cli/``, and
``browser/``'s own ``SmokeContext`` variants.

``DomainStats``/``StepResult``/``step_slug``/``to_jsonable``/``_truncate``
are adapted from ``../apm-sdk-python/tests/smoke/``'s own ``SmokeContext``
(see ``sdk/README.md`` for the differences -- the main one being a
four-way step outcome here, not three, since several named samples have a
documented, expected data gap that is neither a pass nor a real bug).
Each distribution's own ``_context.py`` defines its own ``SmokeContext``
around these -- ``sdk/``'s needs ``degrade_on``/``ctx.data``'s richer
registry; ``cli/``'s records a subprocess invocation instead of an
in-process coroutine; the shape doesn't generalize past these pieces.
"""

from __future__ import annotations

import dataclasses
import enum
import re
from typing import Any, Literal

StepStatus = Literal["passed", "skipped", "degraded", "failed"]


@dataclasses.dataclass
class DomainStats:
    """Tally for one domain, rendered as one row of ``index.md``'s
    per-domain table."""

    ran: int = 0
    skipped: int = 0
    degraded: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    unexpected: int = 0


@dataclasses.dataclass
class StepResult:
    """One row of ``index.md``'s per-domain checklist."""

    step: str
    status: StepStatus
    label: str
    has_detail: bool
    note: str = ""


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def step_slug(step: str) -> str:
    """A stable, URL-fragment-safe anchor id for one step name, so
    ``index.md``'s checklist can link straight to ``<domain>.md``'s matching
    ``### `step` `` section."""
    return _SLUG_RE.sub("-", step.lower()).strip("-")


def to_jsonable(obj: Any) -> Any:
    """Best-effort conversion for a ``ctx.call``/``ctx.run`` result dump
    into ``<domain>.md`` -- handles this SDK's own frozen dataclasses
    (``Connection``/``Workload``/``Version``/...) and enums directly; any
    other object a caller passes through is left to ``json.dumps``'s own
    ``default=str`` fallback (set by every caller of this function) rather
    than special-cased here. A caller pre-shapes anything with a live
    object nested inside it (``RestorableUnit.content``, a
    ``Repository``, ...) into a plain summary before calling in, where
    practical, but this function stays safe either way: a dataclass's
    fields are walked one at a time through this same function, recursing
    only into the cases handled below -- deliberately **not**
    ``dataclasses.asdict()``, which deep-copies every field via
    ``copy.deepcopy`` and crashes on one holding a live, unpicklable
    resource (a ``Repository``'s ``threading.Lock``, an open ``aiosqlite``
    connection, ...). A field left unhandled here is passed straight to
    ``json.dumps``'s own ``default=str`` fallback instead.
    """
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: to_jsonable(getattr(obj, f.name)) for f in dataclasses.fields(obj)}
    if isinstance(obj, enum.Enum):
        return obj.value
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    if isinstance(obj, dict):
        return {k: to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, bytes):
        preview = obj[:64].hex()
        return preview if len(obj) <= 64 else f"{preview}...(+{len(obj) - 64} bytes)"
    return obj


def _truncate(data: Any, limit: int = 5) -> tuple[Any, str | None]:
    """Truncates a long list result before it's dumped into ``<domain>.md``
    -- the same instinct as the reference project's own
    ``_truncate_result``, just without its tuple-of-(items, total) shape
    (nothing here returns paginated ``(items, total)`` pairs)."""
    if isinstance(data, list) and len(data) > limit:
        return data[:limit], f"...({len(data) - limit} more, {len(data)} total)"
    return data, None
