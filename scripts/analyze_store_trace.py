"""Summarize a smoke ``store_trace.jsonl``: per-sample call counts and store
time, and which ObjectStore calls repeat (same method, path, offset, length)
grouped by path kind and by the ``<domain>.<step>`` tag that issued them.

    uv run python scripts/analyze_store_trace.py tests/smoke/reports/sdk/<ts>/store_trace.jsonl

Read-only; prints a text report to stdout. Events with no ``step`` field are
attributed to ``(untagged)``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

_NORMALIZERS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"@ActiveProtectData/[^/]+"), "@ActiveProtectData/<repo>"),
    (re.compile(r"@ActiveProtectVault/saas/\d+/[^/]+"), "@ActiveProtectVault/saas/<n>/<id>"),
    (re.compile(r"userKey/[^/]+"), "userKey/<id>"),
    (re.compile(r"/db/([a-z_]+)\.\d+(-wal|-shm)?"), r"/db/\1.N\2"),
    (re.compile(r"(repo_info|repo_transaction)\.\d+"), r"\1.N"),
    (re.compile(r"suppl_transaction_ids/\d+"), "suppl_transaction_ids/N"),
    (re.compile(r"/Pool/\d+/\d+/\d+\.buk(\.\d+)?"), "/Pool/*/*/*.buk"),
    (re.compile(r"/Composition/\d+/\d+\.com/c\d+\.\d+"), "/Composition/*/*.com/c*.*"),
)

UNTAGGED = "(untagged)"


def normalize_path(path: str) -> str:
    """Collapse ids, sequence suffixes and bucket numbers so repeats group by kind."""
    for pattern, replacement in _NORMALIZERS:
        path = pattern.sub(replacement, path)
    return path


def load_events(lines: Iterable[str]) -> list[dict[str, Any]]:
    """Parse JSONL trace lines, skipping blanks."""
    return [json.loads(line) for line in lines if line.strip()]


def _key(event: dict[str, Any]) -> tuple[Any, ...]:
    return (event["sample"], event["method"], event["path"], event["offset"], event["length"])


def per_sample(events: Sequence[dict[str, Any]]) -> dict[str, dict[str, float]]:
    """``{sample: {"calls": n, "store_time_s": t, "extra_calls": repeats}}``."""
    totals: dict[str, dict[str, float]] = defaultdict(lambda: {"calls": 0, "store_time_s": 0.0, "extra_calls": 0})
    seen: set[tuple[Any, ...]] = set()
    for event in events:
        row = totals[event["sample"]]
        row["calls"] += 1
        row["store_time_s"] += event["elapsed"]
        key = _key(event)
        if key in seen:
            row["extra_calls"] += 1
        seen.add(key)
    return dict(totals)


def repeats_by_kind(events: Sequence[dict[str, Any]]) -> list[tuple[str, str, int, int, float]]:
    """``(method, normalized path, extra calls, total calls, extra store seconds)``, largest first."""
    seen: set[tuple[Any, ...]] = set()
    extra: Counter[tuple[str, str]] = Counter()
    extra_time: defaultdict[tuple[str, str], float] = defaultdict(float)
    total: Counter[tuple[str, str]] = Counter()
    for event in events:
        kind = (event["method"], normalize_path(event["path"]))
        total[kind] += 1
        key = _key(event)
        if key in seen:
            extra[kind] += 1
            extra_time[kind] += event["elapsed"]
        seen.add(key)
    rows = [(m, p, n, total[(m, p)], extra_time[(m, p)]) for (m, p), n in extra.items()]
    return sorted(rows, key=lambda row: -row[2])


def repeats_by_step(events: Sequence[dict[str, Any]]) -> list[tuple[str, int]]:
    """Extra (repeated) calls attributed to the step that issued the repeat."""
    seen: set[tuple[Any, ...]] = set()
    extra: Counter[str] = Counter()
    for event in events:
        key = _key(event)
        if key in seen:
            extra[event.get("step") or UNTAGGED] += 1
        seen.add(key)
    return extra.most_common()


def render(events: Sequence[dict[str, Any]], top: int = 15) -> str:
    """The full text report."""
    methods = Counter(e["method"] for e in events)
    repeated = sum(row[2] for row in repeats_by_kind(events))
    out = [
        f"calls: {len(events)}  ({', '.join(f'{m}={n}' for m, n in methods.most_common())})",
        f"repeated calls: {repeated} ({100 * repeated / max(1, len(events)):.1f}%)",
        "",
        f"{'sample':32}{'calls':>8}{'store s':>10}{'extra':>7}",
    ]
    for sample, row in sorted(per_sample(events).items(), key=lambda kv: -kv[1]["calls"]):
        out.append(f"{sample:32}{int(row['calls']):8}{row['store_time_s']:10.1f}{int(row['extra_calls']):7}")
    out += ["", f"top {top} repeated kinds (extra/total, extra store s):"]
    for method, path, n, tot, seconds in repeats_by_kind(events)[:top]:
        out.append(f"  {n:6}/{tot:<7} {seconds:8.1f}s  {method:8} {path}")
    out += ["", "repeats by issuing step:"]
    out += [f"  {n:6}  {step}" for step, n in repeats_by_step(events)[:top]]
    return "\n".join(out)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("trace", type=Path, help="path to a store_trace.jsonl")
    parser.add_argument("--top", type=int, default=15, help="rows per section (default: 15)")
    args = parser.parse_args(argv)
    with args.trace.open(encoding="utf-8") as f:
        events = load_events(f)
    print(render(events, args.top))
    return 0


if __name__ == "__main__":
    sys.exit(main())
