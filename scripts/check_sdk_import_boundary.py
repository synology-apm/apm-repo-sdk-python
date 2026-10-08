"""SDK public-surface check: code outside the SDK imports it only through its
public modules -- the top-level ``synology_apm_repo.sdk`` package plus
``sdk.export``, ``sdk.presentation``, ``sdk.profiles`` and ``sdk.diagnostics`` (see
``PUBLIC_MODULES``). Covers the CLI and Browser packages' production source
and ``examples/``, the SDK's own reference consumers: what they need is what
any other frontend needs, so it belongs in the public surface rather than in
a per-file exception. Tests are white-box and not covered.

An import must name a public module exactly: a submodule of one is as
internal as any other, whether named as the import's module
(``from sdk.presentation.format import ...``) or pulled in by name
(``from synology_apm_repo.sdk import dedup``). Not covered: reaching a
submodule through attribute access on a public import
(``sdk.dedup.repository``), which needs type information to see. Ruff's
banned-import rule (TID251) cannot express this: it is a denylist, while this
is an allowlist (a new SDK module is internal until it is made public).

The same walk checks that the two frontends stay independent of each other:
the CLI never imports the Browser package, the Browser never imports the CLI,
and ``examples/`` imports neither (``ARCHITECTURE.md``: each depends only on
the SDK).

Exit code 0 = clean; non-zero = violations printed to stderr.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

_CLI_ROOT = ROOT / "packages/synology-apm-repo-cli/src/synology_apm_repo/cli"
_BROWSER_ROOT = ROOT / "packages/synology-apm-repo-browser/src/synology_apm_repo/browser"
_EXAMPLES_ROOT = ROOT / "examples"

#: Directories whose every ``.py`` file is checked.
SOURCE_ROOTS = [_CLI_ROOT, _BROWSER_ROOT, _EXAMPLES_ROOT]

#: The frontend packages each source root must not import.
FORBIDDEN_FRONTENDS: dict[Path, tuple[str, ...]] = {
    _CLI_ROOT: ("synology_apm_repo.browser",),
    _BROWSER_ROOT: ("synology_apm_repo.cli",),
    _EXAMPLES_ROOT: ("synology_apm_repo.cli", "synology_apm_repo.browser"),
}

#: The SDK's source root, for telling an imported name that is a submodule
#: from one a module exports. Fixed here, not derived from ``ROOT``, which the
#: tests point at a tmp tree.
_SDK_SRC = Path(__file__).parent.parent / "packages/synology-apm-repo-sdk/src"

#: The SDK's public modules; an ``sdk`` import must equal one of them.
PUBLIC_MODULES = frozenset(
    {
        "synology_apm_repo.sdk",
        "synology_apm_repo.sdk.diagnostics",
        "synology_apm_repo.sdk.export",
        "synology_apm_repo.sdk.presentation",
        "synology_apm_repo.sdk.profiles",
    }
)


def _is_sdk_module(dotted: str) -> bool:
    """Whether ``dotted`` names a module or package in the SDK's source tree.
    Matched against the directory's own entries, so a case-insensitive
    filesystem doesn't take an exported ``Catalog`` for the ``catalog``
    package."""
    *parents, name = dotted.split(".")
    directory = _SDK_SRC.joinpath(*parents)
    if not directory.is_dir():
        return False
    entries = {entry.name for entry in directory.iterdir()}
    return f"{name}.py" in entries or (name in entries and (directory / name / "__init__.py").is_file())


def _sdk_imports(tree: ast.Module) -> list[tuple[int, str]]:
    """Every ``sdk``-rooted module named in a module-level or nested
    ``from ... import ...``/``import ...`` statement, with its line number --
    including a submodule a ``from`` statement imports by name."""
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("synology_apm_repo.sdk"):
            found.append((node.lineno, node.module))
            found.extend(
                (node.lineno, f"{node.module}.{alias.name}")
                for alias in node.names
                if alias.name != "*" and _is_sdk_module(f"{node.module}.{alias.name}")
            )
        elif isinstance(node, ast.Import):
            found.extend(
                (node.lineno, alias.name) for alias in node.names if alias.name.startswith("synology_apm_repo.sdk")
            )
    return found


def _imported_modules(tree: ast.Module) -> list[tuple[int, str]]:
    """Every module an absolute import statement (module-level or nested)
    names, with its line number -- for ``from a import b``, both ``a`` and
    ``a.b``, since ``b`` may itself be a package."""
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and not node.level:
            found.append((node.lineno, node.module))
            found.extend((node.lineno, f"{node.module}.{alias.name}") for alias in node.names if alias.name != "*")
        elif isinstance(node, ast.Import):
            found.extend((node.lineno, alias.name) for alias in node.names)
    return found


def _is_within(module: str, packages: tuple[str, ...]) -> bool:
    return any(module == package or module.startswith(f"{package}.") for package in packages)


def main() -> int:
    violations: list[str] = []
    frontend_violations: list[str] = []
    for root in SOURCE_ROOTS:
        forbidden = FORBIDDEN_FRONTENDS.get(root, ())
        for path in sorted(root.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            # as_posix(): this report is read (and asserted on) the same way on
            # every platform, so it should not switch to backslashes on Windows.
            where = path.relative_to(ROOT).as_posix()
            violations.extend(
                f"{where}:{lineno}: imports {imported!r}"
                for lineno, imported in _sdk_imports(tree)
                if imported not in PUBLIC_MODULES
            )
            frontend_violations.extend(
                f"{where}:{lineno}: imports {imported!r}"
                for lineno, imported in _imported_modules(tree)
                if _is_within(imported, forbidden)
            )
    if frontend_violations:
        print("ERROR: a frontend imports another frontend; each depends only on the SDK:", file=sys.stderr)
        for violation in frontend_violations:
            print(f"  {violation}", file=sys.stderr)
    if violations:
        print(
            "ERROR: import of an internal SDK module. Import from a public module "
            f"({', '.join(sorted(PUBLIC_MODULES))}); if the name isn't exported there, export it:",
            file=sys.stderr,
        )
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
    if violations or frontend_violations:
        return 1
    print("OK: every import of the SDK outside it goes through a public module, and no frontend imports another.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
