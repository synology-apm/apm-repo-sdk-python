"""Exporting a folder: ``plan_tree_export`` maps each item below it to a
destination, ``preflight_tree`` checks those destinations against the disk,
and ``run_tree_export`` writes the items.

Names come from the backup (possibly from a file inside a guest), so every
destination goes through ``safe_export_join``, which rejects rather than
renames: an item whose name cannot be one safe path component, or whose
destination another item already takes, is skipped and reported, never
written somewhere else. Only a name the SDK synthesizes
(``Node.export_name``) is sanitized and numbered instead, since the backup
never promised it as a file name.
"""

from __future__ import annotations

import dataclasses
import enum
import os
from collections.abc import Callable
from pathlib import Path

from ..api.export_paths import safe_export_join, safe_file_name
from ..dedup.extent import ExportResult
from ..dedup.local_file_sink import LocalFileSink
from ..errors import NotRestorableError
from ..presentation.export_target import destination_state
from ..presentation.format import pluralize
from ..units.base import Node, UnitKind, UnitProvider
from .export import ExportProgressCallback, run_export


class SkipReason(enum.StrEnum):
    """Why an item is not exported; ``description`` is how the CLI and the
    Browser both word it."""

    MISSING_DATA = "missing_data"
    """The backup refers to data it could not find (a diagnostic placeholder)."""
    UNSAFE_NAME = "unsafe_name"
    """The item's name is not one safe path component on this system."""
    PATH_TAKEN = "path_taken"
    """Another item already takes the destination path."""
    NO_CONTENT = "no_content"
    """The item has no content to export."""

    @property
    def description(self) -> str:
        """How a skipped-item line words this reason, after the item's name."""
        return _SKIP_REASON_TEXT[self]


_SKIP_REASON_TEXT = {
    SkipReason.MISSING_DATA: "the backup refers to data it could not find",
    SkipReason.UNSAFE_NAME: "its name is not a safe file name here",
    SkipReason.PATH_TAKEN: "another item already takes this path",
    SkipReason.NO_CONTENT: "it has no content to export",
}


@dataclasses.dataclass(frozen=True, slots=True)
class TreeItem:
    """One item to export: ``node`` to ``path``, shown to the user as ``relative``."""

    node: Node
    path: Path
    relative: str


@dataclasses.dataclass(frozen=True, slots=True)
class SkippedItem:
    """An item that will not be exported, and why."""

    relative: str
    reason: SkipReason


@dataclasses.dataclass(frozen=True, slots=True)
class TreePlan:
    """The items to export, and the ones skipped while planning."""

    items: list[TreeItem]
    skipped: list[SkippedItem]


async def plan_tree_export(provider: UnitProvider, folder: Node, output: Path) -> TreePlan:
    """Every exportable item below ``folder``, depth-first in provider order, with its destination under ``output``.

    A ``DISK_FILESYSTEM`` container (a disk image's "(filesystem)" browse view) is left out
    without a note: its files are the same bytes as the disk image exported beside it. A
    diagnostic placeholder (data the backup refers to but could not find) is skipped and reported.
    Same-named sibling folders merge into one; files that then share a path are skipped and reported,
    except under a synthesized ``export_name``, which takes the first free `` (2)``, `` (3)``, ...
    variant — compared case-insensitively, since the destination's filesystem may be.
    """
    items: list[TreeItem] = []
    skipped: list[SkippedItem] = []
    claimed_files: set[Path] = set()
    claimed_dirs: set[Path] = set()
    claimed_folded: set[str] = set()

    def first_free(segments: tuple[str, ...], name: str) -> tuple[str, ...]:
        below = (*segments, name)
        number = 1
        while str(safe_export_join(output, *below)).casefold() in claimed_folded:
            number += 1
            below = (*segments, _numbered(name, number))
        return below

    async def walk(node: Node, segments: tuple[str, ...]) -> None:
        for child in await provider.children(node):
            if child.kind is UnitKind.DISK_FILESYSTEM:
                continue
            synthesized = child.export_name if child.is_leaf else None
            below = (*segments, child.name if synthesized is None else safe_file_name(synthesized))
            relative = "/".join(below)
            if child.is_diagnostic:
                skipped.append(SkippedItem(relative, SkipReason.MISSING_DATA))
                continue
            try:
                if synthesized is not None:
                    below = first_free(segments, below[-1])
                    relative = "/".join(below)
                path = safe_export_join(output, *below)
            except ValueError:
                skipped.append(SkippedItem(relative, SkipReason.UNSAFE_NAME))
                continue
            if (
                path in claimed_files
                or (child.is_leaf and path in claimed_dirs)
                or any(parent in claimed_files for parent in path.parents)
            ):
                skipped.append(SkippedItem(relative, SkipReason.PATH_TAKEN))
                continue
            claimed_folded.add(str(path).casefold())
            if child.is_leaf:
                claimed_files.add(path)
                items.append(TreeItem(child, path, relative))
            else:
                claimed_dirs.add(path)
                await walk(child, below)

    await walk(folder, ())
    return TreePlan(items, skipped)


def _numbered(name: str, number: int) -> str:
    """``name`` with `` (number)`` before its extension: ``a.eml`` -> ``a (2).eml``."""
    stem, dot, extension = name.rpartition(".")
    return f"{stem} ({number}).{extension}" if dot and stem else f"{name} ({number})"


class TreeProblemKind(enum.Enum):
    """What stops a folder export before it writes anything."""

    DIRECTORY_IN_THE_WAY = "directory_in_the_way"
    FILE_IN_THE_WAY = "file_in_the_way"
    EXISTS = "exists"


@dataclasses.dataclass(frozen=True, slots=True)
class TreeProblem:
    """The first reason a folder export cannot start.

    Attributes:
        path: The destination that cannot be written, or for ``EXISTS`` the first
            of the ``existing`` destinations.
        blocker: For ``FILE_IN_THE_WAY``, the file standing where a parent folder
            of ``path`` has to be.
        existing: For ``EXISTS``, how many destinations are already taken.
        total: For ``EXISTS``, how many items the plan has.
    """

    kind: TreeProblemKind
    path: Path
    blocker: Path | None = None
    existing: int = 0
    total: int = 0

    def message(self, *, force_hint: bool = False) -> str:
        """This problem as one sentence; ``force_hint`` adds the ``--force``
        advice where the surface has that option."""
        match self.kind:
            case TreeProblemKind.DIRECTORY_IN_THE_WAY:
                return f"{self.path} is a directory — an item cannot replace it"
            case TreeProblemKind.FILE_IN_THE_WAY:
                return f"{self.blocker} is a file, so {self.path} cannot be created under it"
            case TreeProblemKind.EXISTS:
                existing, files = self.existing, pluralize(self.total, "file")
                text = (
                    f"{existing} of {self.total} destination {files} already "
                    f"{pluralize(existing, 'exists', 'exist')}, starting with {self.path}"
                )
                return f"{text} — pass --force to overwrite {pluralize(existing, 'it', 'them')}" if force_hint else text


class _ParentCheck:
    """Finds a file standing where a folder has to be. An existing folder's own parents are folders
    too, so a folder already seen to exist ends the search for everything below it."""

    def __init__(self, known_directories: frozenset[Path] = frozenset()) -> None:
        self.directories = set(known_directories)

    def blocker(self, path: Path, output: Path) -> Path | None:
        for parent in path.parents:
            if parent == output or parent in self.directories:
                return None
            state = destination_state(parent)
            if state == "file":
                return parent
            if state == "directory":
                self.directories.add(parent)
                return None
        return None


@dataclasses.dataclass(frozen=True, slots=True)
class TreePreflight:
    """The outcome of ``preflight_tree``.

    Attributes:
        problem: Why the export cannot start, or ``None`` when it can.
        preexisting: The destinations that were already on disk, which a run
            with ``force`` replaces.
        directories: Folders found to exist, so ``run_tree_export`` need not look again.
    """

    problem: TreeProblem | None
    preexisting: frozenset[Path] = frozenset()
    directories: frozenset[Path] = frozenset()


def preflight_tree(plan: TreePlan, output: Path, *, force: bool) -> TreePreflight:
    """Checks every destination of ``plan`` against the disk, before the first write.

    A directory where an item goes, or a file where one of its parent folders goes,
    is a problem even with ``force``, which only replaces files. Otherwise an
    existing destination is a problem unless ``force`` is set.

    Synchronous, with a few ``stat()`` calls per item: call it through
    ``asyncio.to_thread`` from a UI that must stay responsive on a large plan.
    """
    parents = _ParentCheck()
    preexisting: set[Path] = set()
    existing: list[Path] = []
    for item in plan.items:
        state = destination_state(item.path)
        if state == "directory":
            return TreePreflight(TreeProblem(TreeProblemKind.DIRECTORY_IN_THE_WAY, item.path))
        if (blocker := parents.blocker(item.path, output)) is not None:
            return TreePreflight(TreeProblem(TreeProblemKind.FILE_IN_THE_WAY, item.path, blocker))
        if state == "file":
            preexisting.add(item.path)
            if not force:
                existing.append(item.path)
    directories = frozenset(parents.directories)
    if existing:
        problem = TreeProblem(TreeProblemKind.EXISTS, existing[0], existing=len(existing), total=len(plan.items))
        return TreePreflight(problem, frozenset(preexisting), directories)
    return TreePreflight(None, frozenset(preexisting), directories)


@dataclasses.dataclass(frozen=True, slots=True)
class IncompleteItem:
    """An exported item whose content could only be partly resolved, and why
    (its unit's ``degraded``)."""

    relative: str
    reason: str


@dataclasses.dataclass(frozen=True, slots=True)
class TreeExport:
    """What ``run_tree_export`` did: the items written with their results,
    everything skipped, and which written items are incomplete."""

    exported: list[tuple[TreeItem, ExportResult]]
    skipped: list[SkippedItem]
    incomplete: list[IncompleteItem] = dataclasses.field(default_factory=list)

    @property
    def totals(self) -> ExportResult:
        """Every exported item's result, summed."""
        results = [result for _, result in self.exported]
        return ExportResult(
            bytes_written=sum(r.bytes_written for r in results),
            logical_size=sum(r.logical_size for r in results),
            holes=sum(r.holes for r in results),
            zeros=sum(r.zeros for r in results),
        )


def _file_identity(path: Path) -> tuple[int, int] | None:
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return stat.st_dev, stat.st_ino


async def run_tree_export(
    provider: UnitProvider,
    plan: TreePlan,
    output: Path,
    *,
    preflight: TreePreflight,
    sparse: bool = True,
    keep_partial: bool = False,
    on_item_start: Callable[[int, TreeItem, LocalFileSink], ExportProgressCallback | None] | None = None,
    on_item_done: Callable[[TreeItem, ExportResult], None] | None = None,
) -> TreeExport:
    """Exports ``plan``'s items one at a time, each through its own staged ``LocalFileSink``.

    Names differing only in case are one path on a case-insensitive filesystem,
    which the plan cannot see, so an item whose destination appeared during this
    run (or is a file this run already wrote) is skipped rather than overwritten.
    The first failure stops the run and propagates; items already written stay.

    Args:
        preflight: What ``preflight_tree`` returned for ``plan``.
        sparse: Leave ``ZERO``/``HOLE`` ranges unwritten where the sink allows it.
        keep_partial: Keep an item's ``.part`` file when its export fails or is cancelled.
        on_item_start: Called with the item's index in ``plan.items``, the item, and its
            sink before ``provider.unit()`` opens it, so a caller can say what is in flight
            while that runs; the item may still turn out to have no content and be skipped.
            May return this item's ``progress`` callback for ``run_export``.
        on_item_done: Called after each item is written.

    Returns:
        The items written, the items skipped (the plan's included), and the
        written items that are incomplete.
    """
    exported: list[tuple[TreeItem, ExportResult]] = []
    skipped = list(plan.skipped)
    incomplete: list[IncompleteItem] = []
    written: set[tuple[int, int]] = set()
    parents = _ParentCheck(preflight.directories)
    for index, item in enumerate(plan.items):
        state = destination_state(item.path)
        taken = state == "directory" or (
            state != "missing" and (item.path not in preflight.preexisting or _file_identity(item.path) in written)
        )
        if taken or parents.blocker(item.path, output) is not None:
            skipped.append(SkippedItem(item.relative, SkipReason.PATH_TAKEN))
            continue
        sink = LocalFileSink(item.path, staged=True, keep_partial=keep_partial)
        progress = on_item_start(index, item, sink) if on_item_start is not None else None
        try:
            unit = await provider.unit(item.node)
        except NotRestorableError:
            skipped.append(SkippedItem(item.relative, SkipReason.NO_CONTENT))
            continue
        result = await run_export(unit.content, sink, sparse=sparse, progress=progress)
        if (identity := _file_identity(item.path)) is not None:
            written.add(identity)
        exported.append((item, result))
        if unit.degraded is not None:
            incomplete.append(IncompleteItem(item.relative, unit.degraded))
        if on_item_done is not None:
            on_item_done(item, result)
    return TreeExport(exported, skipped, incomplete)
