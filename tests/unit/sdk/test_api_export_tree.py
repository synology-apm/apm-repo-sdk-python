"""Unit tests for ``sdk/api/export_tree.py``: planning which items of a folder go where, checking the
destinations before the first write, and exporting the items one at a time."""

from __future__ import annotations

import dataclasses
import importlib
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Literal

import pytest

from support.content_fakes import TreeProvider
from support.fakes import faithful_to
from synology_apm_repo.sdk.errors import NotRestorableError
from synology_apm_repo.sdk.export import (
    ExportResult,
    ExportWriter,
    IncompleteItem,
    LocalFileSink,
    SkippedItem,
    SkipReason,
    TreeExport,
    TreeItem,
    TreePlan,
    TreePreflight,
    TreeProblem,
    TreeProblemKind,
    plan_tree_export,
    preflight_tree,
    run_tree_export,
)
from synology_apm_repo.sdk.presentation.export_report import size_summary
from synology_apm_repo.sdk.presentation.export_target import part_path_for
from synology_apm_repo.sdk.units.base import ContentSource, Node, RestorableUnit, UnitKind
from synology_apm_repo.sdk.units.node_ref import NodeRef
from synology_apm_repo.sdk.units.provider_kit import diagnostic_node


def _ref(*segments: str) -> NodeRef:
    return NodeRef("repo", ("ver", *segments))


def _folder(*segments: str, kind: UnitKind | None = None) -> Node:
    return Node(ref=_ref(*segments), name=segments[-1] if segments else "ver", is_leaf=False, kind=kind)


def _file(*segments: str, name: str | None = None, export_name: str | None = None) -> Node:
    return Node(
        ref=_ref(*segments),
        name=name if name is not None else segments[-1],
        is_leaf=True,
        size=1,
        export_name=export_name,
    )


@faithful_to(ContentSource)
class _Content:
    """A ``ContentSource`` that writes fixed bytes."""

    def __init__(self, data: bytes = b"data") -> None:
        self.size = len(data)
        self._data = data

    async def read(self, offset: int = 0, length: int | None = None) -> bytes:
        raise AssertionError("not used in this test")

    def stream(self, block: int = 0) -> AsyncIterator[tuple[int, bytes]]:
        raise AssertionError("not used in this test")

    async def planned_bytes(self, start: int, end: int) -> int:
        return end - start

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: object = None,
        tuning: object = None,
    ) -> ExportResult:
        await sink.write_at(0, self._data)
        return ExportResult(bytes_written=len(self._data), logical_size=len(self._data), holes=0, zeros=0)


def _provider(children: dict[NodeRef, list[Node]], contents: dict[str, _Content] | None = None) -> TreeProvider:
    by_ref = {_ref(*key.split("/")): value for key, value in (contents or {}).items()}
    return TreeProvider(_folder(), children, by_ref)


# -- plan_tree_export ----------------------------------------------------------


async def _plan(provider: TreeProvider, output: Path) -> tuple[list[tuple[str, Path]], list[SkippedItem]]:
    plan = await plan_tree_export(provider, provider.root(), output)
    return [(item.relative, item.path) for item in plan.items], plan.skipped


async def test_the_plan_lists_items_depth_first_at_their_path_below_the_folder(tmp_path: Path) -> None:
    folder_d = _folder("d")
    provider = _provider(
        {
            _ref(): [folder_d, _file("g.txt")],
            _ref("d"): [_file("d", "f.txt"), _folder("d", "e")],
            _ref("d", "e"): [_file("d", "e", "deep.bin")],
        }
    )

    items, skipped = await _plan(provider, tmp_path)

    assert items == [
        ("d/f.txt", tmp_path / "d" / "f.txt"),
        ("d/e/deep.bin", tmp_path / "d" / "e" / "deep.bin"),
        ("g.txt", tmp_path / "g.txt"),
    ]
    assert skipped == []


async def test_an_empty_folder_plans_nothing(tmp_path: Path) -> None:
    assert await _plan(_provider({}), tmp_path) == ([], [])


async def test_a_disk_images_filesystem_view_is_left_out_without_a_note(tmp_path: Path) -> None:
    provider = _provider(
        {
            _ref(): [_file("disk.img"), _folder("(filesystem)", kind=UnitKind.DISK_FILESYSTEM)],
            _ref("(filesystem)"): [_file("(filesystem)", "same-bytes.txt")],
        }
    )

    items, skipped = await _plan(provider, tmp_path)

    assert items == [("disk.img", tmp_path / "disk.img")]
    assert skipped == []


async def test_a_diagnostic_placeholder_is_skipped_and_reported(tmp_path: Path) -> None:
    placeholder = diagnostic_node(_ref("(missing fragments)"), "(2 objects not found)", "x")
    provider = _provider({_ref(): [_file("ok.txt"), placeholder]})

    items, skipped = await _plan(provider, tmp_path)

    assert [relative for relative, _ in items] == ["ok.txt"]
    assert skipped == [SkippedItem("(2 objects not found)", SkipReason.MISSING_DATA)]


@pytest.mark.parametrize("name", ["..", ".", "a/b", "a\\b", "", "bad\x00name"])
async def test_a_name_that_is_not_one_safe_path_component_is_skipped_not_renamed(tmp_path: Path, name: str) -> None:
    provider = _provider({_ref(): [_file("ok.txt"), _file("x", name=name)]})

    items, skipped = await _plan(provider, tmp_path)

    assert [relative for relative, _ in items] == ["ok.txt"]
    assert skipped == [SkippedItem(name, SkipReason.UNSAFE_NAME)]


async def test_a_folder_with_an_unsafe_name_is_skipped_with_everything_below_it(tmp_path: Path) -> None:
    evil = Node(ref=_ref("evil"), name="..", is_leaf=False)
    provider = _provider({_ref(): [evil], _ref("evil"): [_file("evil", "escaped.txt")]})

    items, skipped = await _plan(provider, tmp_path)

    assert items == []
    assert skipped == [SkippedItem("..", SkipReason.UNSAFE_NAME)]


async def test_colliding_paths_keep_the_first_and_report_the_rest(tmp_path: Path) -> None:
    provider = _provider(
        {
            _ref(): [_file("a1", name="a"), _file("a2", name="a"), _folder("b"), _file("b2", name="b")],
            _ref("b"): [_file("b", "inside.txt")],
        }
    )

    items, skipped = await _plan(provider, tmp_path)

    assert [relative for relative, _ in items] == ["a", "b/inside.txt"]
    assert skipped == [
        SkippedItem("a", SkipReason.PATH_TAKEN),
        SkippedItem("b", SkipReason.PATH_TAKEN),
    ]


async def test_a_file_then_a_folder_of_the_same_name_skips_the_folder_and_its_contents(tmp_path: Path) -> None:
    provider = _provider(
        {
            _ref(): [_file("a-file", name="a"), _folder("a")],
            _ref("a"): [_file("a", "inside.txt")],
        }
    )

    items, skipped = await _plan(provider, tmp_path)

    assert [relative for relative, _ in items] == ["a"]
    assert skipped == [SkippedItem("a", SkipReason.PATH_TAKEN)]


async def test_same_named_folders_merge_and_only_colliding_files_are_skipped(tmp_path: Path) -> None:
    provider = _provider(
        {
            _ref(): [_folder("d1", "x"), _folder("d2", "x")],
        }
    )
    provider._children[_ref("d1", "x")] = [_file("d1", "x", "a"), _file("d1", "x", "same")]
    provider._children[_ref("d2", "x")] = [_file("d2", "x", "b"), _file("d2", "x", "same")]

    items, skipped = await _plan(provider, tmp_path)

    assert [relative for relative, _ in items] == ["x/a", "x/same", "x/b"]
    assert skipped == [SkippedItem("x/same", SkipReason.PATH_TAKEN)]


async def test_a_synthesized_export_name_is_used_and_numbered_when_it_repeats(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two mails with one subject both export: a synthesized name is numbered
    on a collision, unlike a real name, which the backup promised."""
    monkeypatch.setattr(sys, "platform", "linux")
    provider = _provider(
        {
            _ref(): [
                _file("m1", name="Re: hi", export_name="Re: hi.eml"),
                _file("m2", name="Re: hi", export_name="Re: hi.eml"),
                _file("m3", name="RE: HI", export_name="RE: HI.eml"),
            ]
        }
    )

    items, skipped = await _plan(provider, tmp_path)

    assert [relative for relative, _path in items] == ["Re: hi.eml", "Re: hi (2).eml", "RE: HI (3).eml"]
    assert skipped == []


async def test_a_synthesized_export_name_is_made_safe_rather_than_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "platform", "win32")
    provider = _provider({_ref(): [_file("m1", name="Q3: plan/review", export_name="Q3: plan/review.eml")]})

    items, skipped = await _plan(provider, tmp_path)

    assert items == [("Q3_ plan_review.eml", tmp_path / "Q3_ plan_review.eml")]
    assert skipped == []


async def test_a_real_name_is_still_skipped_not_numbered_when_a_synthesized_name_took_its_path(
    tmp_path: Path,
) -> None:
    provider = _provider({_ref(): [_file("m1", name="a", export_name="a.eml"), _file("a.eml")]})

    items, skipped = await _plan(provider, tmp_path)

    assert [relative for relative, _path in items] == ["a.eml"]
    assert skipped == [SkippedItem("a.eml", SkipReason.PATH_TAKEN)]


async def test_windows_only_rules_apply_on_windows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    provider = _provider({_ref(): [_file("ok.txt"), _file("c", name="C:foo"), _file("n", name="NUL.txt")]})
    assert [relative for relative, _ in (await _plan(provider, tmp_path))[0]] == ["ok.txt", "C:foo", "NUL.txt"]

    monkeypatch.setattr(sys, "platform", "win32")
    items, skipped = await _plan(provider, tmp_path)

    assert [relative for relative, _ in items] == ["ok.txt"]
    assert [item.relative for item in skipped] == ["C:foo", "NUL.txt"]


def test_tree_item_keeps_the_relative_path_it_was_given() -> None:
    node = _file("a.txt")

    assert TreeItem(node, Path("/out/a.txt"), "a.txt").relative == "a.txt"


# -- preflight_tree ----------------------------------------------------------------


def _plan_of(output: Path, *relatives: str) -> TreePlan:
    return TreePlan([TreeItem(_file(*r.split("/")), output / r, r) for r in relatives], [])


def test_preflight_of_an_empty_destination_is_clear(tmp_path: Path) -> None:
    result = preflight_tree(_plan_of(tmp_path, "a", "d/b"), tmp_path, force=False)

    assert result.problem is None
    assert result.preexisting == frozenset()


def test_preflight_counts_existing_files_and_names_the_first(tmp_path: Path) -> None:
    (tmp_path / "a").write_bytes(b"x")
    (tmp_path / "b").write_bytes(b"x")

    result = preflight_tree(_plan_of(tmp_path, "a", "b", "c"), tmp_path, force=False)

    assert result.problem is not None
    assert (result.problem.kind, result.problem.path) == (TreeProblemKind.EXISTS, tmp_path / "a")
    assert (result.problem.existing, result.problem.total) == (2, 3)


def test_force_accepts_existing_files_and_reports_them_as_preexisting(tmp_path: Path) -> None:
    (tmp_path / "a").write_bytes(b"x")

    result = preflight_tree(_plan_of(tmp_path, "a", "c"), tmp_path, force=True)

    assert result.problem is None
    assert result.preexisting == frozenset({tmp_path / "a"})


def test_a_directory_where_an_item_goes_is_a_problem_even_with_force(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()

    result = preflight_tree(_plan_of(tmp_path, "a"), tmp_path, force=True)

    assert result.problem is not None
    assert (result.problem.kind, result.problem.path) == (TreeProblemKind.DIRECTORY_IN_THE_WAY, tmp_path / "a")


def test_a_file_where_a_parent_folder_goes_is_a_problem_even_with_force(tmp_path: Path) -> None:
    (tmp_path / "d").write_bytes(b"x")

    result = preflight_tree(_plan_of(tmp_path, "d/b"), tmp_path, force=True)

    assert result.problem is not None
    assert result.problem.kind is TreeProblemKind.FILE_IN_THE_WAY
    assert (result.problem.path, result.problem.blocker) == (tmp_path / "d" / "b", tmp_path / "d")


def test_a_hard_problem_wins_over_an_earlier_existing_file(tmp_path: Path) -> None:
    (tmp_path / "a").write_bytes(b"x")
    (tmp_path / "b").mkdir()

    result = preflight_tree(_plan_of(tmp_path, "a", "b"), tmp_path, force=False)

    assert result.problem is not None
    assert result.problem.kind is TreeProblemKind.DIRECTORY_IN_THE_WAY


# -- run_tree_export -------------------------------------------------------------------


async def _run(
    provider: TreeProvider, output: Path, *, force: bool = False, **kwargs: object
) -> tuple[list[str], list[SkippedItem]]:
    plan = await plan_tree_export(provider, provider.root(), output)
    preflight = preflight_tree(plan, output, force=force)
    assert preflight.problem is None
    done = await run_tree_export(provider, plan, output, preflight=preflight, **kwargs)  # type: ignore[arg-type]
    return [item.relative for item, _ in done.exported], done.skipped


async def test_export_tree_writes_each_item_at_its_path(tmp_path: Path) -> None:
    provider = _provider(
        {_ref(): [_folder("d"), _file("g")], _ref("d"): [_file("d", "f")]},
        {"d/f": _Content(b"F"), "g": _Content(b"G")},
    )

    exported, skipped = await _run(provider, tmp_path)

    assert exported == ["d/f", "g"]
    assert skipped == []
    assert (tmp_path / "d" / "f").read_bytes() == b"F"
    assert (tmp_path / "g").read_bytes() == b"G"


async def test_an_item_without_content_is_skipped_and_the_rest_continue(tmp_path: Path) -> None:
    provider = _provider({_ref(): [_file("a"), _file("b")]}, {"b": _Content(b"B")})

    exported, skipped = await _run(provider, tmp_path)

    assert exported == ["b"]
    assert skipped == [SkippedItem("a", SkipReason.NO_CONTENT)]


async def test_an_item_whose_unit_is_degraded_is_written_and_reported_incomplete(tmp_path: Path) -> None:
    class _Degraded(TreeProvider):
        async def unit(self, node: Node) -> RestorableUnit:
            unit = await super().unit(node)
            return dataclasses.replace(unit, degraded="part of it is missing") if node.name == "a" else unit

    provider = _Degraded(
        _folder(), {_ref(): [_file("a"), _file("b")]}, {_ref("a"): _Content(b"A"), _ref("b"): _Content(b"B")}
    )
    plan = await plan_tree_export(provider, provider.root(), tmp_path)

    done = await run_tree_export(provider, plan, tmp_path, preflight=preflight_tree(plan, tmp_path, force=False))

    assert [item.relative for item, _ in done.exported] == ["a", "b"]
    assert done.incomplete == [IncompleteItem("a", "part of it is missing")]
    assert (tmp_path / "a").read_bytes() == b"A"


class _ReportingContent(_Content):
    """Reports its one write to ``progress``."""

    async def export_range(
        self,
        sink: ExportWriter,
        start: int,
        end: int,
        *,
        sparse: bool = True,
        progress: object = None,
        tuning: object = None,
    ) -> ExportResult:
        result = await super().export_range(sink, start, end, sparse=sparse, progress=progress, tuning=tuning)
        assert callable(progress)
        await progress(result.bytes_written)
        return result


async def test_hooks_see_each_item_with_its_sink(tmp_path: Path) -> None:
    provider = _provider(
        {_ref(): [_file("a"), _file("b")]}, {"a": _ReportingContent(b"A"), "b": _ReportingContent(b"BB")}
    )
    started: list[tuple[int, str, Path]] = []
    finished: list[str] = []
    seen: list[tuple[str, int, int]] = []

    def on_start(index: int, item: TreeItem, sink: LocalFileSink) -> object:
        started.append((index, item.relative, sink.path))

        async def progress(done: int, total: int) -> None:
            seen.append((item.relative, done, total))

        return progress

    await _run(
        provider, tmp_path, on_item_start=on_start, on_item_done=lambda item, _result: finished.append(item.relative)
    )

    assert started == [(0, "a", part_path_for(tmp_path / "a")), (1, "b", part_path_for(tmp_path / "b"))]
    assert finished == ["a", "b"]
    assert seen == [("a", 1, 1), ("b", 2, 2)]  # each item's own callback, with its own totals


async def test_a_destination_that_appeared_during_the_run_is_not_overwritten(tmp_path: Path) -> None:
    provider = _provider({_ref(): [_file("a"), _file("b")]}, {"a": _Content(b"A"), "b": _Content(b"B")})
    plan = await plan_tree_export(provider, provider.root(), tmp_path)

    def on_start(index: int, item: TreeItem, sink: LocalFileSink) -> None:
        if index == 0:
            (tmp_path / "b").write_bytes(b"theirs")  # what a case-variant of an earlier item looks like

    done = await run_tree_export(provider, plan, tmp_path, preflight=TreePreflight(None), on_item_start=on_start)

    assert [item.relative for item, _ in done.exported] == ["a"]
    assert done.skipped == [SkippedItem("b", SkipReason.PATH_TAKEN)]
    assert (tmp_path / "b").read_bytes() == b"theirs"


async def test_two_existing_hard_links_are_replaced_as_two_files(tmp_path: Path) -> None:
    (tmp_path / "a").write_bytes(b"OLD")
    try:
        os.link(tmp_path / "a", tmp_path / "b")
    except (OSError, NotImplementedError):
        pytest.skip("this filesystem has no hard links")
    provider = _provider({_ref(): [_file("a"), _file("b")]}, {"a": _Content(b"AAA"), "b": _Content(b"BBB")})

    exported, skipped = await _run(provider, tmp_path, force=True)

    assert (exported, skipped) == (["a", "b"], [])
    assert (tmp_path / "a").read_bytes() == b"AAA"
    assert (tmp_path / "b").read_bytes() == b"BBB"


async def test_a_plain_value_error_from_unit_is_a_failure_not_a_skip(tmp_path: Path) -> None:
    class _Broken(TreeProvider):
        async def unit(self, node: Node) -> RestorableUnit:
            raise ValueError("a parser bug, not a missing content")

    provider = _Broken(_folder(), {_ref(): [_file("a")]}, {})
    plan = await plan_tree_export(provider, provider.root(), tmp_path)

    with pytest.raises(ValueError, match="parser bug"):
        await run_tree_export(provider, plan, tmp_path, preflight=preflight_tree(plan, tmp_path, force=False))


def test_preflight_remembers_the_folders_it_found_and_looks_at_each_parent_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from synology_apm_repo.sdk.presentation import export_target

    module = importlib.import_module("synology_apm_repo.sdk.api.export_tree")

    (tmp_path / "d" / "e").mkdir(parents=True)
    plan = _plan_of(tmp_path, "d/e/a", "d/e/b", "d/e/c")
    looked_at: list[Path] = []
    real = export_target.destination_state

    def counting(path: Path) -> Literal["missing", "file", "directory"]:
        looked_at.append(path)
        return real(path)

    monkeypatch.setattr(module, "destination_state", counting)

    result = preflight_tree(plan, tmp_path, force=False)

    assert result.problem is None
    assert result.directories == frozenset({tmp_path / "d" / "e"})
    assert looked_at.count(tmp_path / "d" / "e") == 1  # three siblings, one stat of their folder
    assert tmp_path / "d" not in looked_at  # an existing folder's own parents are folders too


async def test_run_tree_export_does_not_look_at_a_folder_preflight_already_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from synology_apm_repo.sdk.presentation import export_target

    module = importlib.import_module("synology_apm_repo.sdk.api.export_tree")

    (tmp_path / "d").mkdir()
    provider = _provider(
        {_ref(): [_folder("d")], _ref("d"): [_file("d", "a"), _file("d", "b")]},
        {"d/a": _Content(b"A"), "d/b": _Content(b"B")},
    )
    plan = await plan_tree_export(provider, provider.root(), tmp_path)
    preflight = preflight_tree(plan, tmp_path, force=False)
    looked_at: list[Path] = []
    real = export_target.destination_state

    def counting_again(path: Path) -> Literal["missing", "file", "directory"]:
        looked_at.append(path)
        return real(path)

    monkeypatch.setattr(module, "destination_state", counting_again)

    await run_tree_export(provider, plan, tmp_path, preflight=preflight)

    assert tmp_path / "d" not in looked_at


async def test_a_unit_without_a_content_source_is_skipped_as_no_content(tmp_path: Path) -> None:
    class _Contentless(TreeProvider):
        async def unit(self, node: Node) -> RestorableUnit:
            raise NotRestorableError(f"{node.name!r} has no content")

    provider = _Contentless(_folder(), {_ref(): [_file("a")]}, {})
    plan = await plan_tree_export(provider, provider.root(), tmp_path)

    done = await run_tree_export(provider, plan, tmp_path, preflight=preflight_tree(plan, tmp_path, force=False))

    assert done.exported == []
    assert done.skipped == [SkippedItem("a", SkipReason.NO_CONTENT)]


async def test_a_destination_that_became_a_directory_since_the_preflight_is_skipped(tmp_path: Path) -> None:
    (tmp_path / "a").write_bytes(b"old")
    provider = _provider({_ref(): [_file("a")]}, {"a": _Content(b"A")})
    plan = await plan_tree_export(provider, provider.root(), tmp_path)
    preflight = preflight_tree(plan, tmp_path, force=True)
    (tmp_path / "a").unlink()
    (tmp_path / "a").mkdir()

    done = await run_tree_export(provider, plan, tmp_path, preflight=preflight)

    assert done.exported == []
    assert done.skipped == [SkippedItem("a", SkipReason.PATH_TAKEN)]


def test_every_skip_reason_has_a_description() -> None:
    assert all(reason.description for reason in SkipReason)


class TestTreeProblemMessage:
    def test_directory_in_the_way(self) -> None:
        a = Path("/out/a")
        problem = TreeProblem(TreeProblemKind.DIRECTORY_IN_THE_WAY, a)
        assert problem.message() == f"{a} is a directory — an item cannot replace it"

    def test_file_in_the_way_names_the_blocking_file(self) -> None:
        d = Path("/out/d")
        problem = TreeProblem(TreeProblemKind.FILE_IN_THE_WAY, d / "a", blocker=d)
        assert problem.message() == f"{d} is a file, so {d / 'a'} cannot be created under it"

    def test_exists_adds_the_force_advice_only_when_asked(self) -> None:
        a = Path("/out/a")
        problem = TreeProblem(TreeProblemKind.EXISTS, a, existing=2, total=5)
        plain = f"2 of 5 destination files already exist, starting with {a}"
        assert problem.message() == plain
        assert problem.message(force_hint=True) == f"{plain} — pass --force to overwrite them"


def test_totals_sums_every_exported_item_and_size_summary_words_them() -> None:
    item = TreeItem(node=Node(ref=NodeRef("", ("x",)), name="x", is_leaf=True), relative="x", path=Path("/out/x"))
    done = TreeExport(
        exported=[
            (item, ExportResult(bytes_written=1024, logical_size=4096, holes=2048, zeros=1024)),
            (item, ExportResult(bytes_written=1024, logical_size=1024, holes=0, zeros=0)),
        ],
        skipped=[],
    )
    assert done.totals == ExportResult(bytes_written=2048, logical_size=5120, holes=2048, zeros=1024)
    assert size_summary(done.totals) == "2.0 KiB written, 5.0 KiB logical, 2.0 KiB holes, 1.0 KiB zero-fill"
