"""CLI/TUI import-boundary check: only the SDK's narrow public surface crosses
into ``synology_apm_repo.cli``/``synology_apm_repo.browser`` -- covers each
package's own production source *and* its own ``tests/unit``/``tests/integration``
directory, since a test reaching past the facade is exactly as much a real
dependency as production code doing it, just invisible to every other
reviewer/tool until something actually breaks. ``tests/unit/sdk``/
``tests/integration/sdk`` are deliberately not covered -- those test SDK
internals directly, which is the whole point of that half of the suite, not
a CLI/TUI-boundary question.

The contract this checks (also documented in ``synology_apm_repo.sdk.api``'s
module docstring): CLI and TUI code should only ever import from
``sdk.api``, plus a short, fixed list of cross-cutting/foundational
modules (``units.base``, ``units.node_ref``, ``presentation``, ``storage``, ``profiles``, ``errors``,
``identifiers``) that either predate the facade or are needed to construct
values a facade method takes/returns (e.g. ``Session.open_remote()``'s
``ObjectStore`` argument). Everything else under ``sdk`` (``catalog``,
``dedup``, ``format``, most of ``units``) is an implementation detail the
facade exists to hide — reaching past it from CLI/TUI widens the blast
radius of any future change to those internals, with nothing else in the
toolchain (ruff's rule set has no banned-import check) to catch a new
violation before review.

The exceptions hardcoded below match the ones ``sdk.api``'s docstring
names explicitly.

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
_UNIT_GOTO_HELPER = ("synology_apm_repo.sdk.units.resolve",)
_UNIT_LIST_OVERVIEW_HELPER = ("synology_apm_repo.sdk.units.saas.site",)
_UNIT_DISK_FS_SIBLING_HELPER = ("synology_apm_repo.sdk.units.device_disk_fs",)
_DEDUP_REPO_FIXTURE = ("synology_apm_repo.sdk.dedup.repository",)
_REPO_INFO_FIXTURE = ("synology_apm_repo.sdk.format.repo_info",)
_SAAS_STREAM_CACHE_FIXTURE = ("synology_apm_repo.sdk.units.saas.stream",)
_DIAGNOSTICS_MODULE = ("synology_apm_repo.sdk.diagnostics",)
_DEVICE_MODULE_MONKEYPATCH = ("synology_apm_repo.sdk.units.device",)
_CONCURRENCY_MODULE = ("synology_apm_repo.sdk.concurrency",)
_DUMP_FORMAT_PRIMITIVES = (
    "synology_apm_repo.sdk.format.addressing",
    "synology_apm_repo.sdk.format.bucket",
    "synology_apm_repo.sdk.format.compression",
    "synology_apm_repo.sdk.format.const",
    "synology_apm_repo.sdk.format.redundancy",
)

#: Per-importing-module extra allowances, each scoped to exactly the one
#: call site that needs it, since each is call-site-specific rather than
#: general CLI/TUI vocabulary. To add an entry: find (or add) a shared
#: tuple above for the reason, then key it by the importing module's own
#: dotted name (production packages resolve normally; a test file's is
#: `tests.<unit|integration>.<cli|browser>.<module>`, matching this
#: script's own `SOURCE_ROOTS`/`_module_name`).
EXCEPTIONS: dict[str, tuple[str, ...]] = {
    "synology_apm_repo.cli.commands.dump": _DIAGNOSTICS_MODULE,
    "synology_apm_repo.browser.screens.unit_screen": (
        *_UNIT_GOTO_HELPER,
        *_UNIT_LIST_OVERVIEW_HELPER,
        *_CONCURRENCY_MODULE,
    ),
    "synology_apm_repo.browser.core.unit.select": _UNIT_LIST_OVERVIEW_HELPER,
    "synology_apm_repo.browser.app": _CONCURRENCY_MODULE,
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
    # ever holds an already-opened instance handed to it internally. Each
    # also needs SaasStreamCache: constructing a real Catalog directly
    # (these are whitebox Catalog subclasses/fakes, not the facade's own
    # Repository.catalogs()) now requires one, the same "no facade
    # equivalent" reasoning as DedupRepo -- production CLI/TUI code never
    # constructs a Catalog directly either.
    "tests.unit.cli.test_cli_browse": (*_DEDUP_REPO_FIXTURE, *_SAAS_STREAM_CACHE_FIXTURE),
    "tests.unit.cli.test_cli_ls": (*_DEDUP_REPO_FIXTURE, *_SAAS_STREAM_CACHE_FIXTURE),
    "tests.unit.cli.test_cli_tree": (*_DEDUP_REPO_FIXTURE, *_SAAS_STREAM_CACHE_FIXTURE),
    "tests.unit.browser.test_browser_browse_screen_gaps": (*_DEDUP_REPO_FIXTURE, *_SAAS_STREAM_CACHE_FIXTURE),
    # RepoInfo construction for a synthetic Connection/repo fixture -- no
    # facade equivalent; same reasoning as DedupRepo above.
    "tests.unit.cli.test_cli_doctor_report": _REPO_INFO_FIXTURE,
    "tests.unit.cli.test_cli_profile_option": _REPO_INFO_FIXTURE,
    # A real (not duck-typed) Catalog for verbose-label formatting, which
    # reaches catalog.info/.connection -- same DedupRepo/RepoInfo/
    # SaasStreamCache fixture depth as the exceptions immediately above,
    # applied to a single test in this file rather than the whole
    # module's own fixtures.
    "tests.unit.browser.test_browser_core_browse_select": (
        *_DEDUP_REPO_FIXTURE,
        *_REPO_INFO_FIXTURE,
        *_SAAS_STREAM_CACHE_FIXTURE,
    ),
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
    # These test UnitScreen/select.py directly. _UNIT_LIST_OVERVIEW_HELPER
    # here is the same call site as those modules' own production
    # exceptions above, exercised from a test instead. _UNIT_DISK_FS_
    # SIBLING_HELPER has no production counterpart -- neither module
    # imports device_disk_fs in production at all -- but these tests need
    # DISK_FS_SIBLING_REF_ATTR itself to construct/assert the sibling-ref
    # attribute device_disk_fs.py attaches to a node.
    "tests.unit.browser.test_browser_unit_screen_disk_fs_nesting": _UNIT_DISK_FS_SIBLING_HELPER,
    "tests.unit.browser.test_browser_unit_screen_gaps": (*_UNIT_DISK_FS_SIBLING_HELPER, *_UNIT_LIST_OVERVIEW_HELPER),
    "tests.unit.browser.test_browser_core_unit_select": (
        *_UNIT_DISK_FS_SIBLING_HELPER,
        *_UNIT_LIST_OVERVIEW_HELPER,
    ),
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
    seen_modules: set[str] = set()
    for root, root_package in SOURCE_ROOTS:
        for path in sorted(root.rglob("*.py")):
            module_name = _module_name(path, root, root_package)
            seen_modules.add(module_name)
            allowed_extra = EXCEPTIONS.get(module_name, ())
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for lineno, imported in _sdk_imports(tree):
                if _matches(imported, ALLOWED_SDK_PREFIXES) or _matches(imported, allowed_extra):
                    continue
                # as_posix(): this report is read (and asserted on) the same way on
                # every platform, so it should not switch to backslashes on Windows.
                violations.append(f"{path.relative_to(ROOT).as_posix()}:{lineno}: imports {imported!r}")

    # A key that no longer names a real module (a rename/move/delete that
    # forgot to update EXCEPTIONS alongside it) would otherwise pass
    # silently forever -- nothing above ever looks a stale key up, so
    # there's no other way this script would ever notice one on its own.
    stale_keys = sorted(k for k in EXCEPTIONS if k not in seen_modules)

    if violations or stale_keys:
        if violations:
            print(
                "ERROR: import outside the SDK's documented CLI/TUI surface (see sdk/api/__init__.py). "
                "If this import is a genuine, scoped exception (no facade alternative exists), add it to "
                "this script's own EXCEPTIONS dict instead of repointing it:",
                file=sys.stderr,
            )
            for violation in violations:
                print(f"  {violation}", file=sys.stderr)
        if stale_keys:
            print(
                "ERROR: EXCEPTIONS key(s) that no longer match any real module -- "
                "the module was renamed/moved/deleted without updating this dict:",
                file=sys.stderr,
            )
            for key in stale_keys:
                print(f"  {key}", file=sys.stderr)
        return 1

    print("OK: every CLI/TUI import of the SDK stays within the documented surface.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
