"""Version consistency check: synology-apm-repo-{sdk,cli,browser} share one lockstep version.

Pass 1 -- every package's ``project.version`` in its own pyproject.toml must equal
         synology-apm-repo-sdk's version (the three packages are released together).

Pass 2 -- the ``synology-apm-repo-sdk==X.Y.Z`` pin in synology-apm-repo-cli's and
         synology-apm-repo-browser's ``project.dependencies`` must equal
         synology-apm-repo-sdk's version (catches a version bump that updated a
         package's own ``version`` field but left its dependency pin on the SDK stale).

Exit code 0 = clean; non-zero = errors printed to stderr.
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent.parent
PACKAGES = ("synology-apm-repo-sdk", "synology-apm-repo-cli", "synology-apm-repo-browser")
PYPROJECT_PATHS = {name: ROOT / "packages" / name / "pyproject.toml" for name in PACKAGES}


def _load(name: str) -> dict[str, Any]:
    with open(PYPROJECT_PATHS[name], "rb") as f:
        return tomllib.load(f)


def _pin(dependencies: list[str], package: str) -> str | None:
    prefix = f"{package}=="
    for dep in dependencies:
        if dep.startswith(prefix):
            return dep.split("==", 1)[1]
    return None


def main() -> int:
    data = {name: _load(name) for name in PACKAGES}
    versions = {name: data[name]["project"]["version"] for name in PACKAGES}
    sdk_version = versions["synology-apm-repo-sdk"]

    errors: list[str] = []
    for name in ("synology-apm-repo-cli", "synology-apm-repo-browser"):
        if versions[name] != sdk_version:
            errors.append(
                f"{name}/pyproject.toml version={versions[name]!r} does not match "
                f"synology-apm-repo-sdk version={sdk_version!r}"
            )

        pin = _pin(data[name]["project"]["dependencies"], "synology-apm-repo-sdk")
        if pin is None:
            errors.append(f"{name}/pyproject.toml is missing a synology-apm-repo-sdk== dependency pin")
        elif pin != sdk_version:
            errors.append(
                f"{name}/pyproject.toml pins synology-apm-repo-sdk=={pin}, but "
                f"synology-apm-repo-sdk version={sdk_version!r}"
            )

    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1

    print(f"OK: all packages at version {sdk_version!r}; dependency pins consistent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
