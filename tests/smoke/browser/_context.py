"""Run state for one ``python -m tests.smoke.browser`` invocation.

Shaped like ``sdk/_context.py``'s ``SmokeContext`` (``ctx.call``/
``.check``/``.skip``), not ``cli/_context.py``'s (``ctx.run``): a browser
phase drives a real Textual ``Pilot`` directly, awaiting SDK/UI actions in
the same process the way ``sdk/``'s phases do -- there is no subprocess
boundary here to record an exit code for.
"""

from __future__ import annotations

import io
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

from .._context import DomainStats, StepResult, StepStatus, _truncate, step_slug, to_jsonable

T = TypeVar("T")

#: The domains this tool's phases are split across -- see
#: ``phases/_<domain>.py`` and this package's README for what each covers.
DOMAINS = (
    "navigate",
    "diagnostics_and_verbose",
    "export_worklist",
    "hex_preview",
    "key_dialog",
    "remote_connect",
    "help_screen",
)


@dataclass
class SmokeContext:
    report_dir: Path
    data: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, DomainStats] = field(default_factory=lambda: {d: DomainStats() for d in DOMAINS})
    step_results: dict[str, list[StepResult]] = field(default_factory=lambda: {d: [] for d in DOMAINS})

    _files: dict[str, io.TextIOWrapper] = field(default_factory=dict, repr=False)
    _emitted: set[tuple[str, str]] = field(default_factory=set, repr=False)

    def __post_init__(self) -> None:
        self.report_dir.mkdir(parents=True, exist_ok=True)
        for domain in DOMAINS:
            f = (self.report_dir / f"{domain}.md").open("w", encoding="utf-8")
            f.write(f"# {domain} -- Browser smoke test report\n\n")
            self._files[domain] = f

    def _mark(self, domain: str, step: str) -> None:
        self._emitted.add((domain, step))

    async def call(
        self,
        domain: str,
        step: str,
        coro: Callable[[], Awaitable[T]],
        *,
        degrade_on: tuple[type[Exception], ...] = (),
        note: str = "",
    ) -> T | None:
        """Await ``coro()``, recorded into ``<domain>.md`` as step
        ``step`` -- same shape as ``sdk/``'s ``ctx.call``: a
        ``degrade_on`` exception is recorded DEGRADED and returns
        ``None``; any other exception is recorded ``unexpected`` (a real
        bug) and also returns ``None``, so the calling phase can keep
        going either way."""
        stats = self.stats[domain]
        self._mark(domain, step)
        stats.ran += 1
        result: T | None = None
        error_text: str | None = None
        status: StepStatus = "passed"
        try:
            result = await coro()
        except degrade_on as exc:
            status = "degraded"
            stats.degraded += 1
            error_text = f"{type(exc).__name__}: {exc}"
        except Exception as exc:  # deliberately broad -- see sdk/_context.py's ctx.call docstring
            status = "failed"
            stats.unexpected += 1
            error_text = f"{type(exc).__name__}: {exc}"
        self._write_call(domain, step, result, error_text, status=status, note=note)
        return result

    def check(self, domain: str, step: str, condition: bool, *, note: str = "") -> bool:
        """Record a pure boolean assertion (no I/O, never raises) --
        PASSED/FAILED, never SKIPPED/DEGRADED. ``condition`` must already be
        a safe value to evaluate -- anything that could itself raise (an
        attribute chain into a real domain object, ...) belongs inside its
        own ``ctx.call``-wrapped helper first."""
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

    def _write_call(
        self,
        domain: str,
        step: str,
        result: Any,
        error_text: str | None,
        *,
        status: StepStatus,
        note: str = "",
    ) -> None:
        slug = step_slug(step)
        f = self._files[domain]
        f.write(f'<a id="{slug}"></a>\n')
        f.write(f"### `{step}`\n\n")
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

        label = {
            "passed": "PASSED",
            "degraded": f"DEGRADED: {error_text}",
            "failed": f"FAILED: {error_text}",
        }[status]
        self.step_results[domain].append(StepResult(step, status=status, label=label, has_detail=True, note=note))

    def close(self) -> None:
        for f in self._files.values():
            f.close()
