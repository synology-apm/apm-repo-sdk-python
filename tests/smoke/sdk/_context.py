"""Run state for one ``python -m tests.smoke.sdk`` invocation."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass

from .._context import CallContext
from .._shared_refs import RepoInfo
from .._trace_step import trace_step

#: One ``phases/_<domain>.py`` each.
DOMAINS = ("catalog", "device", "fs", "saas", "diagnostics")


@dataclass
class SmokeContext(CallContext):
    DOMAINS = DOMAINS
    TITLE = "SDK"

    def _step_scope(self, domain: str, step: str) -> AbstractContextManager[object]:
        return trace_step(domain, step)


__all__ = ["DOMAINS", "RepoInfo", "SmokeContext"]
