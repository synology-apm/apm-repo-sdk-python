"""SDK layer-direction check: inside ``synology_apm_repo.sdk``, dependencies
only point downward through ``ARCHITECTURE.md``'s layers.

Every rule but the cycle check counts every import alike (module-level,
function-local, ``TYPE_CHECKING``) -- a lazy or type-only import is still a real
dependency between two layers. Rules:

* **Layer order.** A module in one layer never imports from a layer above
  it (``format < storage < dedup < catalog < units < api``;
  ``diagnostics.py`` and ``export.py`` are ``api``'s siblings and rank with
  it).
* **Leaves.** The ``LEAVES`` packages import nothing outside the leaves, so
  every layer can depend on them.
* **Side module.** ``profiles`` (the stores it builds) imports only the
  leaves and ``storage``.
* **Every top-level module is classified** as one of the above, so a new one
  can't go unchecked.
* **Content layer.** Within ``units/``, ``units/content/`` reaches the Unit
  layer only through ``units.base``'s ``ContentSource`` contract and
  ``units.provider_kit``'s shared helpers.
* **No import cycles.** No group of modules imports itself back through
  module-level or function-local imports. A function-local import counts: it
  is how a cycle gets dodged at import time. ``TYPE_CHECKING`` imports do
  not count, since they never run.

Exit code 0 = clean; non-zero = violations printed to stderr.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Iterator
from pathlib import Path

ROOT = Path(__file__).parent.parent

SDK_ROOT = ROOT / "packages/synology-apm-repo-sdk/src/synology_apm_repo/sdk"

#: Layered top-level packages, lowest first.
LAYER_ORDER = ("format", "storage", "dedup", "catalog", "units", "api", "diagnostics", "export")

#: Modules that rank with another layer rather than forming their own.
_SAME_RANK_AS = {"diagnostics": "api", "export": "api"}

#: Cross-cutting packages that import no layered package at all.
LEAVES = (
    "_util",
    "errors",
    "identifiers",
    "findings",
    "asynccache",
    "cachemanager",
    "concurrency",
    "positional_io",
    "presentation",
)

#: Packages outside the layer stack with their own allowed imports, beyond the leaves.
_SIDE_PACKAGES = {"profiles": ("storage",)}

#: The only ``units`` module prefixes ``units/content/`` may import.
_CONTENT_ALLOWED_UNITS_PREFIXES = (("units", "content"), ("units", "base"), ("units", "provider_kit"))


def _rank(top: str) -> int | None:
    top = _SAME_RANK_AS.get(top, top)
    return LAYER_ORDER.index(top) if top in LAYER_ORDER else None


def _module_parts(path: Path, root: Path) -> list[str]:
    parts = [*path.relative_to(root).with_suffix("").parts]
    return parts[:-1] if parts[-1] == "__init__" else parts


def _from_target(node: ast.ImportFrom, package_parts: list[str]) -> list[str] | None:
    """The SDK-relative dotted module an ``ImportFrom`` names (``["dedup",
    "chunk_walk"]``; empty for ``from . import x`` at the SDK root), or
    ``None`` for an import from outside the SDK."""
    if node.level:
        up = node.level - 1
        base = package_parts[: len(package_parts) - up] if up else package_parts
        return [*base, *(node.module.split(".") if node.module else [])]
    if node.module and (node.module == "synology_apm_repo.sdk" or node.module.startswith("synology_apm_repo.sdk.")):
        return node.module.split(".")[2:]
    return None


def _imported_modules(tree: ast.Module, parts: list[str], *, is_package: bool) -> list[tuple[int, list[str]]]:
    """Every SDK-relative dotted import target (``["dedup", "chunk_walk"]``)
    named anywhere in ``tree``, with its line number."""
    package_parts = parts if is_package else parts[:-1]
    found: list[tuple[int, list[str]]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            target = _from_target(node, package_parts)
            if target is None:
                continue
            if not target or (node.level and not node.module):
                # ``from . import x`` / ``from synology_apm_repo.sdk import x``: the alias is the module.
                found.extend((node.lineno, [*target, alias.name]) for alias in node.names)
            else:
                found.append((node.lineno, target))
        elif isinstance(node, ast.Import):
            found.extend(
                (node.lineno, alias.name.split(".")[2:])
                for alias in node.names
                if alias.name.startswith("synology_apm_repo.sdk.")
            )
    return found


def _is_type_checking_guard(node: ast.If) -> bool:
    return "TYPE_CHECKING" in ast.unparse(node.test)


def _import_edges(
    tree: ast.Module, parts: list[str], *, is_package: bool, known: set[str]
) -> Iterator[tuple[int, str, str]]:
    """``(line, imported module, kind)`` for every import of a module in
    ``known`` (dotted SDK module names, packages included). ``kind`` is
    ``"module-level"``, ``"function-local"`` or ``"type-checking"``.
    ``from pkg import name`` is an import of ``pkg`` (its ``__init__`` runs)
    and, when ``name`` is itself a module, of that module too."""
    package_parts = parts if is_package else parts[:-1]

    def walk(node: ast.AST, kind: str) -> Iterator[tuple[int, str, str]]:
        if isinstance(node, ast.ImportFrom):
            target = _from_target(node, package_parts)
            if target is not None:
                dotted = ".".join(target)
                names = [dotted, *(f"{dotted}.{alias.name}".lstrip(".") for alias in node.names)]
                yield from ((node.lineno, name, kind) for name in names if name in known)
            return
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = ".".join(alias.name.split(".")[2:]) if alias.name.startswith("synology_apm_repo.sdk.") else ""
                if name in known:
                    yield node.lineno, name, kind
            return
        if isinstance(node, ast.If) and _is_type_checking_guard(node):
            for guarded in node.body:
                yield from walk(guarded, "type-checking")
            for other in node.orelse:
                yield from walk(other, kind)
            return
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            kind = "function-local"
        for child in ast.iter_child_nodes(node):
            yield from walk(child, kind)

    yield from walk(tree, "module-level")


def _strongly_connected(graph: dict[str, set[str]]) -> list[list[str]]:
    """The groups of two or more modules that can all reach each other
    (Tarjan's algorithm)."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    groups: list[list[str]] = []

    def visit(node: str) -> None:
        index[node] = low[node] = len(index)
        stack.append(node)
        on_stack.add(node)
        for other in graph.get(node, ()):
            if other not in index:
                visit(other)
                low[node] = min(low[node], low[other])
            elif other in on_stack:
                low[node] = min(low[node], index[other])
        if low[node] == index[node]:
            group = []
            while True:
                member = stack.pop()
                on_stack.discard(member)
                group.append(member)
                if member == node:
                    break
            if len(group) > 1:
                groups.append(sorted(group))

    for node in list(graph):
        if node not in index:
            visit(node)
    return sorted(groups)


def find_import_cycles(root: Path | None = None) -> list[str]:
    """One report line per group of modules that import each other in a
    cycle, module-level and function-local imports both counted."""
    root = SDK_ROOT if root is None else root
    paths = {".".join(_module_parts(path, root)): path for path in sorted(root.rglob("*.py"))}
    paths.pop("", None)
    known = set(paths)
    graph: dict[str, set[str]] = {name: set() for name in known}
    site: dict[tuple[str, str], tuple[int, str]] = {}
    for name, path in paths.items():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for lineno, imported, kind in _import_edges(
            tree, name.split("."), is_package=path.name == "__init__.py", known=known
        ):
            if kind == "type-checking" or imported == name:
                continue
            graph[name].add(imported)
            # Prefer reporting the import that hides a cycle.
            if (name, imported) not in site or kind == "function-local":
                site[(name, imported)] = (lineno, kind)
    reports = []
    for group in _strongly_connected(graph):
        members = set(group)
        edges = [
            f"{src} -> {dst} ({paths[src].relative_to(root).as_posix()}:{site[(src, dst)][0]}, {site[(src, dst)][1]})"
            for src in group
            for dst in sorted(graph[src] & members)
        ]
        reports.append(f"import cycle among {', '.join(group)}: " + "; ".join(edges))
    return reports


def find_violations(root: Path | None = None) -> list[str]:
    root = SDK_ROOT if root is None else root
    violations: list[str] = []
    for path in sorted(root.rglob("*.py")):
        parts = _module_parts(path, root)
        if not parts:
            continue
        top = parts[0]
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        relative = path.relative_to(root).as_posix()
        if top not in LEAVES and top not in _SIDE_PACKAGES and _rank(top) is None:
            violations.append(f"{relative}: top-level module {top!r} is in no layer, leaf or side package")
            continue
        for lineno, target in _imported_modules(tree, parts, is_package=path.name == "__init__.py"):
            if not target:
                continue
            target_top = target[0]
            dotted = ".".join(target)
            if top in LEAVES:
                if target_top not in LEAVES:
                    violations.append(f"{relative}:{lineno}: leaf package {top!r} imports {dotted!r}")
                continue
            if top in _SIDE_PACKAGES:
                if target_top not in LEAVES and target_top not in (top, *_SIDE_PACKAGES[top]):
                    violations.append(f"{relative}:{lineno}: {top!r} imports {dotted!r}")
                continue
            own_rank, target_rank = _rank(top), _rank(target_top)
            if own_rank is not None and target_rank is not None and target_rank > own_rank:
                violations.append(f"{relative}:{lineno}: {top!r} layer imports higher layer {dotted!r}")
            if (
                parts[:2] == ["units", "content"]
                and target[0] == "units"
                and not any(tuple(target[: len(prefix)]) == prefix for prefix in _CONTENT_ALLOWED_UNITS_PREFIXES)
            ):
                violations.append(f"{relative}:{lineno}: Content layer imports Unit layer module {dotted!r}")
    return [*violations, *find_import_cycles(root)]


def main() -> int:
    violations = find_violations()
    if violations:
        print(
            "ERROR: an SDK import points upward through ARCHITECTURE.md's layers or closes an import cycle "
            "(see scripts/check_sdk_layers.py for the rules):",
            file=sys.stderr,
        )
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        return 1
    print("OK: every SDK import points downward through the documented layers, with no import cycles.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
