"""Browser layer check: inside ``synology_apm_repo.browser``, each layer below
``screens/`` imports only what it builds on (the browser README's "MVU"
section).

* ``view/`` imports nothing else in the package.
* ``widgets/`` builds only on the leaves; ``core/`` on ``view/``;
  ``runtime/`` on ``core/``, ``view/`` and ``widgets/``.
* ``strings`` and ``content_preview/`` are leaves every layer but ``view/``
  may use; ``content_preview/`` imports nothing else in the package.
* Nothing else in the package (``screens/``, ``app``, ``keymap``, ...) is
  importable from these layers.

Every import counts alike (module-level, function-local, ``TYPE_CHECKING``),
relative imports resolved.

Exit code 0 = clean; non-zero = violations printed to stderr.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

BROWSER_ROOT = ROOT / "packages/synology-apm-repo-browser/src/synology_apm_repo/browser"

_PACKAGE = "synology_apm_repo.browser"

_LEAVES = {"strings", "content_preview"}

#: Each layer below ``screens/``, and the top-level names under the package it may import.
ALLOWED = {
    "view": {"view"},
    "content_preview": {"content_preview"},
    "widgets": {"widgets"} | _LEAVES,
    "core": {"core", "view"} | _LEAVES,
    "runtime": {"runtime", "core", "view", "widgets"} | _LEAVES,
}


def imported_layers(path: Path, root: Path) -> list[tuple[int, str]]:
    """``(line, top-level name under the package)`` for every import of the
    browser package in ``path`` (a module under ``root``)."""
    module_parts = [_PACKAGE, *path.relative_to(root).with_suffix("").parts]
    found: list[tuple[int, str]] = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), filename=str(path))):
        if not isinstance(node, ast.Import | ast.ImportFrom):
            continue
        if isinstance(node, ast.ImportFrom):
            if node.level:
                base = module_parts[: len(module_parts) - node.level]
                module = ".".join([*base, *([node.module] if node.module else [])])
            else:
                module = node.module or ""
            # ``from <package> import screens`` names its layer in the alias.
            names = [f"{module}.{alias.name}" for alias in node.names] if module == _PACKAGE else [module]
        else:
            names = [alias.name for alias in node.names]
        found.extend(
            (node.lineno, name.removeprefix(f"{_PACKAGE}.").split(".")[0])
            for name in names
            if name.startswith(f"{_PACKAGE}.")
        )
    return found


def find_violations(root: Path | None = None) -> list[str]:
    root = BROWSER_ROOT if root is None else root
    violations = [f"{layer}/: layer directory is missing" for layer in ALLOWED if not (root / layer).is_dir()]
    for layer, allowed in ALLOWED.items():
        for path in sorted((root / layer).rglob("*.py")):
            violations.extend(
                f"{path.relative_to(root).as_posix()}:{lineno}: {layer}/ imports {imported!r}"
                for lineno, imported in imported_layers(path, root)
                if imported not in allowed
            )
    return violations


def main() -> int:
    violations = find_violations()
    if violations:
        print(
            "ERROR: a browser layer imports a layer it doesn't build on (see scripts/check_browser_layers.py):",
            file=sys.stderr,
        )
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        return 1
    print("OK: every browser layer imports only what it builds on.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
