"""CLI/TUI import-boundary check: only the SDK's narrow public surface crosses
into ``synology_apm_repo.cli``/``synology_apm_repo.browser`` -- covers each
package's own production source *and* its own ``tests/unit``/``tests/integration``
directory, since a test reaching past the facade is exactly as much a real
dependency as production code doing it, just invisible to every other
reviewer/tool until something actually breaks. ``tests/unit/sdk``/
``tests/integration/sdk`` are deliberately not covered -- those test SDK
internals directly, which is the whole point of that half of the suite, not
a CLI/TUI-boundary question.

``synology_apm_repo.sdk.api``'s own module docstring states the contract:
CLI and TUI code should only ever import from ``sdk.api``, plus a short,
fixed list of cross-cutting/foundational modules (``units.base``,
``units.node_ref``, ``presentation``, ``storage``, ``profiles``, ``errors``,
``identifiers``) that either predate the facade or are needed to construct
values a facade method takes/returns (e.g. ``Session.open_remote()``'s
``ObjectStore`` argument). Everything else under ``sdk`` (``catalog``,
``dedup``, ``format``, most of ``units``) is an implementation detail the
facade exists to hide — reaching past it from CLI/TUI widens the blast
radius of any future change to those internals, with nothing else in the
toolchain (ruff's rule set has no banned-import check) to catch a new
violation before review.

Three exceptions are hardcoded below, matching the ones ``sdk.api``'s own
docstring names explicitly: the CLI's ``dump`` command (a diagnostics-only
escape hatch already isolated into its own sibling module, ``sdk.diagnostics``,
rather than folded into the ``api`` package), the TUI's ``UnitScreen``
(three Unit-layer node-navigation helpers with no Repository-layer
equivalent), and the TUI's own entry point, ``browser.app``, calling
``concurrency.preload_resource_tracker()`` before ``App.run()`` (see that
call site's own comment). All three are call-site-specific, not general
CLI/TUI vocabulary, which is why they're exceptions here rather than
additions to the allowed prefix list above.

Exit code 0 = clean; non-zero = violations printed to stderr.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

#: (source root, dotted package prefix the files under it resolve to). The
#: ``tests.*`` prefixes aren't real importable packages -- they only exist so
#: the per-file ``EXCEPTIONS`` lookup below has something to key on, the same
#: as the production packages' own dotted module names.
SOURCE_ROOTS = [
    (ROOT / "packages/synology-apm-repo-cli/src/synology_apm_repo/cli", "synology_apm_repo.cli"),
    (ROOT / "packages/synology-apm-repo-browser/src/synology_apm_repo/browser", "synology_apm_repo.browser"),
    (ROOT / "tests/unit/cli", "tests.unit.cli"),
    (ROOT / "tests/unit/browser", "tests.unit.browser"),
    (ROOT / "tests/integration/cli", "tests.integration.cli"),
    (ROOT / "tests/integration/browser", "tests.integration.browser"),
]

#: A ``sdk`` import is allowed when it equals one of these, or is a
#: dotted-submodule of one (``sdk.presentation.format`` matches
#: ``sdk.presentation``).
ALLOWED_SDK_PREFIXES = (
    "synology_apm_repo.sdk.api",
    "synology_apm_repo.sdk.errors",
    "synology_apm_repo.sdk.identifiers",
    "synology_apm_repo.sdk.presentation",
    "synology_apm_repo.sdk.profiles",
    "synology_apm_repo.sdk.storage",
    "synology_apm_repo.sdk.units.base",
    "synology_apm_repo.sdk.units.node_ref",
)

#: Shared allowance tuples, reused below by every entry that needs the
#: identical depth for the identical reason -- kept as one definition each
#: so a future rename/edit to one doesn't have to be hand-copied into
#: every duplicate.
_UNIT_SCREEN_HELPERS = (
    "synology_apm_repo.sdk.units.device_disk_fs",
    "synology_apm_repo.sdk.units.resolve",
    "synology_apm_repo.sdk.units.saas.site",
)
_DEDUP_REPO_FIXTURE = ("synology_apm_repo.sdk.dedup.repository",)
_REPO_INFO_FIXTURE = ("synology_apm_repo.sdk.format.repo_info",)
_DIAGNOSTICS_MODULE = ("synology_apm_repo.sdk.diagnostics",)
_DEVICE_MODULE_MONKEYPATCH = ("synology_apm_repo.sdk.units.device",)
_DUMP_FORMAT_PRIMITIVES = (
    "synology_apm_repo.sdk.format.addressing",
    "synology_apm_repo.sdk.format.bucket",
    "synology_apm_repo.sdk.format.compression",
    "synology_apm_repo.sdk.format.const",
    "synology_apm_repo.sdk.format.redundancy",
)

#: Per-importing-module extra allowances, each scoped to exactly the one
#: call site that needs it -- see this module's own docstring. To add an
#: entry: find (or add) a shared tuple above for the reason, then key it
#: by the importing module's own dotted name (production packages resolve
#: normally; a test file's is `tests.<unit|integration>.<cli|browser>.<module>`,
#: matching this script's own `SOURCE_ROOTS`/`_module_name`).
EXCEPTIONS: dict[str, tuple[str, ...]] = {
    "synology_apm_repo.cli.commands.dump": _DIAGNOSTICS_MODULE,
    "synology_apm_repo.browser.screens.unit_screen": _UNIT_SCREEN_HELPERS,
    "synology_apm_repo.browser.app": ("synology_apm_repo.sdk.concurrency",),
    # Test-only whitebox exceptions -- each reaches past the facade for a
    # name with no facade equivalent (unlike a real boundary violation,
    # where a facade alternative already exists and the import should
    # just be repointed to it).
    "tests.unit.browser.test_browser_browse_screen_labels": ("synology_apm_repo.sdk.units.dispatch",),
    "tests.unit.browser.test_browser_content_preview": (
        "synology_apm_repo.sdk.units.content.saas_calendar",
        "synology_apm_repo.sdk.units.content.saas_contact",
        "synology_apm_repo.sdk.units.content.saas_teams_chat",
    ),
    "tests.integration.cli.test_cli_canonical_ref_roundtrip": _DEVICE_MODULE_MONKEYPATCH,
    "tests.integration.cli.test_cli_tree": _DEVICE_MODULE_MONKEYPATCH,
    # Fixture/fake construction needing DedupRepo itself (e.g. a
    # _FakeDedupRepo standing in for one) -- no facade equivalent exists;
    # production CLI/TUI code never imports the class name at all, only
    # ever holds an already-opened instance handed to it internally.
    "tests.unit.cli.test_cli_browse": _DEDUP_REPO_FIXTURE,
    "tests.unit.cli.test_cli_ls": _DEDUP_REPO_FIXTURE,
    "tests.unit.cli.test_cli_tree": _DEDUP_REPO_FIXTURE,
    "tests.unit.browser.test_browser_browse_screen_gaps": _DEDUP_REPO_FIXTURE,
    # RepoInfo construction for a synthetic Connection/repo fixture -- no
    # facade equivalent; same reasoning as DedupRepo above.
    "tests.unit.cli.test_cli_doctor_report": _REPO_INFO_FIXTURE,
    "tests.unit.cli.test_cli_profile_option": _REPO_INFO_FIXTURE,
    # test_cli_dump.py tests the CLI's own dump command, which itself
    # carries the identical exception above (raw format-level inspection
    # is this command's whole purpose) -- its test needs the same depth
    # to build the byte fixtures dump parses.
    "tests.unit.cli.test_cli_dump": (*_DIAGNOSTICS_MODULE, *_DUMP_FORMAT_PRIMITIVES),
    "tests.integration.cli.test_cli_dump": _DIAGNOSTICS_MODULE,
    # A raw --object-db-id regression test, deliberately exercising the
    # lower-level identifier/format primitives that flag surfaces --
    # matches this command's own diagnostics-level depth, same rationale
    # as test_cli_dump.py above.
    "tests.integration.cli.test_object_db_id_cli": (
        "synology_apm_repo.sdk.catalog.connection",
        "synology_apm_repo.sdk.catalog.version",
        "synology_apm_repo.sdk.catalog.workload",
        *_DEDUP_REPO_FIXTURE,
        *_DUMP_FORMAT_PRIMITIVES,
        "synology_apm_repo.sdk.format.chunkmap",
    ),
    # These two test UnitScreen directly and need the identical
    # node-navigation helpers UnitScreen's own production exception above
    # already names -- same call site, exercised from its test instead of
    # from the screen itself.
    "tests.unit.browser.test_browser_unit_screen_disk_fs_nesting": _UNIT_SCREEN_HELPERS,
    "tests.unit.browser.test_browser_unit_screen_gaps": _UNIT_SCREEN_HELPERS,
}


def _matches(module: str, prefixes: tuple[str, ...]) -> bool:
    return any(module == prefix or module.startswith(f"{prefix}.") for prefix in prefixes)


def _module_name(path: Path, root: Path, root_package: str) -> str:
    relative = path.relative_to(root).with_suffix("")
    parts = [*relative.parts]
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join([root_package, *parts]) if parts else root_package


def _sdk_imports(tree: ast.Module) -> list[tuple[int, str]]:
    """Every ``sdk``-rooted module named in a module-level or nested
    ``from ... import ...``/``import ...`` statement, with its line number."""
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("synology_apm_repo.sdk"):
            found.append((node.lineno, node.module))
        elif isinstance(node, ast.Import):
            found.extend(
                (node.lineno, alias.name) for alias in node.names if alias.name.startswith("synology_apm_repo.sdk")
            )
    return found


def main() -> int:
    violations: list[str] = []
    for root, root_package in SOURCE_ROOTS:
        for path in sorted(root.rglob("*.py")):
            module_name = _module_name(path, root, root_package)
            allowed_extra = EXCEPTIONS.get(module_name, ())
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for lineno, imported in _sdk_imports(tree):
                if _matches(imported, ALLOWED_SDK_PREFIXES) or _matches(imported, allowed_extra):
                    continue
                # as_posix(): this report is read (and asserted on) the same way on
                # every platform, so it should not switch to backslashes on Windows.
                violations.append(f"{path.relative_to(ROOT).as_posix()}:{lineno}: imports {imported!r}")

    if violations:
        print(
            "ERROR: import outside the SDK's documented CLI/TUI surface (see sdk/api/__init__.py). "
            "If this import is a genuine, scoped exception (no facade alternative exists), add it to "
            "this script's own EXCEPTIONS dict rather than repointing it -- see EXCEPTIONS' own comment:",
            file=sys.stderr,
        )
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        return 1

    print("OK: every CLI/TUI import of the SDK stays within the documented surface.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
