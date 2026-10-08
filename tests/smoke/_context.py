"""Step/result bookkeeping shared by ``sdk/``, ``cli/`` and ``browser/``'s
``SmokeContext``s.

``ReportContext`` owns the per-domain report files, stats and checklist and
records checks and skips; ``CallContext`` adds ``ctx.call`` for an in-process
coroutine step (``sdk/``, ``browser/``), while ``cli/`` records a subprocess
invocation instead. A step has four outcomes (passed/skipped/degraded/
failed): several named samples have a documented, expected data gap that is
neither a pass nor a real bug.
"""

from __future__ import annotations

import contextlib
import dataclasses
import enum
import io
import json
import re
from collections.abc import Awaitable, Callable, Iterable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, ClassVar, Literal, TypeVar

T = TypeVar("T")

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
    """Best-effort conversion of a step result for its ``<domain>.md`` dump:
    dataclasses, enums, containers and bytes; anything else is left to the
    caller's ``json.dumps(..., default=str)``.

    Walks a dataclass's fields itself rather than using
    ``dataclasses.asdict()``, whose ``deepcopy`` crashes on a field holding a
    live resource (a lock, an open ``aiosqlite`` connection).
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
    """Truncates a long list result before it's dumped into ``<domain>.md``;
    returns ``(data, note)`` where ``note`` is ``None`` if nothing was cut."""
    if isinstance(data, list) and len(data) > limit:
        return data[:limit], f"...({len(data) - limit} more, {len(data)} total)"
    return data, None


@dataclasses.dataclass
class ReportContext:
    """Run state for one smoke-tool invocation: the ``data`` registry the
    bootstrap populates and every phase reads, plus one ``<domain>.md``
    report per ``DOMAINS`` entry and the stats/checklist behind ``index.md``."""

    DOMAINS: ClassVar[tuple[str, ...]]
    TITLE: ClassVar[str]

    report_dir: Path
    data: dict[str, Any] = dataclasses.field(default_factory=dict, kw_only=True)
    stats: dict[str, DomainStats] = dataclasses.field(init=False)
    step_results: dict[str, list[StepResult]] = dataclasses.field(init=False)

    _files: dict[str, io.TextIOWrapper] = dataclasses.field(default_factory=dict, init=False, repr=False)
    _emitted: set[tuple[str, str]] = dataclasses.field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        self.stats = {d: DomainStats() for d in self.DOMAINS}
        self.step_results = {d: [] for d in self.DOMAINS}
        self.report_dir.mkdir(parents=True, exist_ok=True)
        for domain in self.DOMAINS:
            f = (self.report_dir / f"{domain}.md").open("w", encoding="utf-8")
            f.write(f"# {domain} -- {self.TITLE} smoke test report\n\n")
            self._files[domain] = f

    def _mark(self, domain: str, step: str) -> None:
        self._emitted.add((domain, step))

    def check(self, domain: str, step: str, condition: bool, *, note: str = "") -> bool:
        """Record a pure boolean assertion (no I/O, never raises) --
        PASSED/FAILED, never SKIPPED/DEGRADED. ``condition`` must already be
        a safe value to evaluate -- anything that could itself raise belongs
        inside its own recorded step first."""
        stats = self.stats[domain]
        self._mark(domain, step)
        if condition:
            stats.checks_passed += 1
            status: StepStatus = "passed"
            label = "PASSED"
        else:
            stats.checks_failed += 1
            status = "failed"
            label = "FAILED"
        self.step_results[domain].append(StepResult(step, status=status, label=label, has_detail=False, note=note))
        return condition

    def skip(self, domain: str, step: str, reason: str) -> None:
        """Record a conditional skip -- prerequisite data this sample set
        legitimately doesn't have, not a hard failure."""
        self._mark(domain, step)
        self.stats[domain].skipped += 1
        self.step_results[domain].append(
            StepResult(step, status="skipped", label=f"SKIPPED: {reason}", has_detail=False)
        )

    def skip_remaining(self, domain: str, steps: Iterable[str], *, reason: str) -> None:
        """Skip every step in ``steps`` not already emitted in ``domain`` --
        a clean all-skipped report when no sample is configured at all."""
        for step in steps:
            if (domain, step) not in self._emitted:
                self.skip(domain, step, reason)

    def _open_section(self, domain: str, step: str) -> io.TextIOWrapper:
        f = self._files[domain]
        f.write(f'<a id="{step_slug(step)}"></a>\n')
        f.write(f"### `{step}`\n\n")
        return f

    def close(self) -> None:
        for f in self._files.values():
            f.close()


@dataclasses.dataclass
class CallContext(ReportContext):
    """``ReportContext`` recording in-process coroutine steps (``ctx.call``)."""

    def _step_scope(self, domain: str, step: str) -> AbstractContextManager[object]:
        """Wraps one ``ctx.call`` step; a subclass adds tracing."""
        return contextlib.nullcontext()

    async def call(
        self,
        domain: str,
        step: str,
        coro: Callable[[], Awaitable[T]],
        *,
        degrade_on: tuple[type[Exception], ...] = (),
        note: str = "",
    ) -> T | None:
        """Await ``coro()``, recorded into ``<domain>.md`` as step ``step``.

        A ``degrade_on`` exception is recorded DEGRADED (a known,
        sample-specific data gap -- see that sample's own comment in your
        ``smoke_samples.toml``) and returns ``None``. Any other exception is
        recorded ``unexpected`` (a real bug -- deliberately broader than the
        SDK's own errors, since a misconfigured ``smoke_samples.toml`` path
        can raise a plain ``OSError`` first) and also returns ``None``, so
        the calling phase can keep going either way.
        """
        stats = self.stats[domain]
        self._mark(domain, step)
        stats.ran += 1
        result: T | None = None
        error_text: str | None = None
        status: StepStatus = "passed"
        try:
            with self._step_scope(domain, step):
                result = await coro()
        except degrade_on as exc:
            status = "degraded"
            stats.degraded += 1
            error_text = f"{type(exc).__name__}: {exc}"
        except Exception as exc:  # noqa: BLE001 - deliberately broad -- see docstring
            status = "failed"
            stats.unexpected += 1
            error_text = f"{type(exc).__name__}: {exc}"
        self._write_call(domain, step, result, error_text, status=status, note=note)
        return result

    def _write_call(
        self, domain: str, step: str, result: Any, error_text: str | None, *, status: StepStatus, note: str = ""
    ) -> None:
        f = self._open_section(domain, step)
        if error_text is not None:
            f.write(f"- result: {error_text}\n")
        else:
            display, trunc_note = _truncate(result)
            f.write("```json\n")
            f.write(json.dumps(to_jsonable(display), indent=2, default=str))
            f.write("\n```\n")
            if trunc_note:
                f.write(f"\n{trunc_note}\n")
        f.write("\n")
        f.flush()

        # call() only produces these three; "skipped" is ctx.skip()'s own.
        label = {
            "passed": "PASSED",
            "degraded": f"DEGRADED: {error_text}",
            "failed": f"FAILED: {error_text}",
        }[status]
        self.step_results[domain].append(StepResult(step, status=status, label=label, has_detail=True, note=note))
