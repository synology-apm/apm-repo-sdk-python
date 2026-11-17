"""Lock source check: every package in uv.lock comes from PyPI or this workspace.

A dependency resolved from anywhere else -- a git repository, a URL, a local
path outside ``packages/``, another index -- would enter a release without
the review a PyPI release gets. Each ``[[package]]`` entry's ``source`` must
be either the PyPI registry or an editable workspace member under
``packages/``.

Exit code 0 = clean; non-zero = errors printed to stderr.
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent.parent
LOCK_PATH = ROOT / "uv.lock"
PYPI = "https://pypi.org/simple"


def _source_error(package: dict[str, Any]) -> str | None:
    """Why ``package``'s source is not allowed, or ``None`` when it is."""
    name = package.get("name", "<unnamed>")
    source = package.get("source", {})
    if source.get("registry") == PYPI:
        return None
    editable = source.get("editable")
    if isinstance(editable, str) and editable.startswith("packages/"):
        return None
    return f"{name} {package.get('version', '')}: source {source!r} is neither {PYPI} nor a workspace member"


def main() -> int:
    with open(LOCK_PATH, "rb") as f:
        packages = tomllib.load(f).get("package", [])

    errors = [error for package in packages if (error := _source_error(package)) is not None]
    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1

    print(f"OK: all {len(packages)} locked packages come from PyPI or the workspace.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
