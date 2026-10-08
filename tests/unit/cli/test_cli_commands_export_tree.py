"""Unit tests for ``export <folder ref> -o DIR`` against a fake repository (the planning, preflight and
per-item loop it drives are tested in ``tests/unit/sdk/test_api_export_tree.py``)."""

from __future__ import annotations

import asyncio
import dataclasses
import errno
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Literal, cast

import pytest

from support.cli import invoke
from support.content_fakes import TreeProvider
from support.fakes import faithful_to
from synology_apm_repo.sdk.api import NodeFrame, RawView, Repository
from synology_apm_repo.sdk.errors import DataCorruptError, NotFoundError
from synology_apm_repo.sdk.export import ExportResult, ExportWriter
from synology_apm_repo.sdk.units.base import ContentSource, Node, RestorableUnit, UnitKind, UnitProvider
from synology_apm_repo.sdk.units.node_ref import NodeRef
from unit.cli.export_fakes import item_frame
from unit.cli.session_fakes import install_fake_session


def _ref(*segments: str) -> NodeRef:
    return NodeRef("repo", ("ver", *segments))


def _folder(*segments: str, kind: UnitKind | None = None) -> Node:
    return Node(ref=_ref(*segments), name=segments[-1] if segments else "ver", is_leaf=False, kind=kind)


def _file(*segments: str, name: str | None = None) -> Node:
    return Node(ref=_ref(*segments), name=name if name is not None else segments[-1], is_leaf=True, size=1)


@faithful_to(ContentSource)
class _Content:
    """A ``ContentSource`` whose export writes ``data``, then raises ``behaviour`` when one is given."""

    def __init__(self, data: bytes = b"data", behaviour: BaseException | None = None) -> None:
        self.size = len(data)
        self._data = data
        self._behaviour = behaviour

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
        if self._behaviour is not None:
            raise self._behaviour
        return ExportResult(bytes_written=len(self._data), logical_size=len(self._data), holes=0, zeros=0)


def _provider(children: dict[NodeRef, list[Node]], contents: dict[str, _Content] | None = None) -> TreeProvider:
    by_ref = {_ref(*key.split("/")): value for key, value in (contents or {}).items()}
    return TreeProvider(_folder(), children, by_ref)


# -- export <folder ref> -o DIR ---------------------------------------------------


@faithful_to(Repository)
class _FakeRepo:
    def __init__(self, folder: Node, provider: TreeProvider) -> None:
        self._folder = folder
        self._provider = provider

    async def resolve(self, ref: str | NodeRef, *, raw: RawView | None = None) -> NodeFrame:
        return NodeFrame(cast(Any, self._provider), self._folder)


def _install(monkeypatch: pytest.MonkeyPatch, provider: TreeProvider, *, folder: Node | None = None) -> None:
    install_fake_session(monkeypatch, [_FakeRepo(folder or provider.root(), provider)])


def _export(tmp_path: Path, *args: str, global_flags: tuple[str, ...] = ()) -> Any:
    return invoke([*global_flags, "export", "somewhere#folder", "-o", str(tmp_path / "out"), *args], exit_code=None)


def _two_files() -> TreeProvider:
    return _provider(
        {_ref(): [_folder("d"), _file("top.txt")], _ref("d"): [_file("d", "f.bin")]},
        {"d/f.bin": _Content(b"inner"), "top.txt": _Content(b"top")},
    )


def test_exporting_a_folder_writes_each_item_at_its_path_below_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, _two_files())

    result = _export(tmp_path)

    assert result.exit_code == 0, result.output
    assert (tmp_path / "out" / "d" / "f.bin").read_bytes() == b"inner"
    assert (tmp_path / "out" / "top.txt").read_bytes() == b"top"
    assert sorted(path.name for path in (tmp_path / "out").rglob("*") if path.is_file()) == ["f.bin", "top.txt"]
    assert " ".join(result.stdout.split()).startswith("exported 2 files to ")
    assert "8 B written, 8 B logical, 0 B holes, 0 B zero-fill)" in " ".join(result.stdout.split())
    assert result.stderr == ""


def test_quiet_drops_the_folder_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, _two_files())

    result = _export(tmp_path, global_flags=("--quiet",))

    assert result.exit_code == 0, result.output
    assert result.stdout == ""
    assert (tmp_path / "out" / "top.txt").read_bytes() == b"top"


def test_an_empty_folder_exports_nothing_and_succeeds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, _provider({}))

    result = _export(tmp_path)

    assert result.exit_code == 0, result.output
    assert " ".join(result.stdout.split()).startswith("exported 0 files to ")


def test_an_unsafe_name_is_skipped_reported_and_makes_the_command_exit_1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = _provider(
        {_ref(): [_file("ok.txt"), _file("bad", name="../escape.txt")]},
        {"ok.txt": _Content(b"ok"), "bad": _Content(b"bad")},
    )
    _install(monkeypatch, provider)

    result = _export(tmp_path)

    assert result.exit_code == 1
    assert (tmp_path / "out" / "ok.txt").read_bytes() == b"ok"
    assert not (tmp_path / "escape.txt").exists() and not list(tmp_path.rglob("escape.txt"))
    assert result.stderr.splitlines() == [
        "skipped ../escape.txt: its name is not a safe file name here",
        "error: 1 item was not exported",
    ]
    assert " ".join(result.stdout.split()).startswith("exported 1 file to ")


def test_quiet_does_not_hide_a_skipped_item(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, _provider({_ref(): [_file("bad", name="..")]}))

    result = _export(tmp_path, global_flags=("-q",))

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "skipped .." in result.stderr


def test_an_incomplete_item_is_exported_and_reported_without_failing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _Degraded(TreeProvider):
        async def unit(self, node: Node) -> RestorableUnit:
            unit = await super().unit(node)
            return dataclasses.replace(unit, degraded="1 of 2 parts are missing") if node.name == "disk.img" else unit

    _install(
        monkeypatch,
        _Degraded(
            _folder(),
            {_ref(): [_file("disk.img"), _file("ok.txt")]},
            {_ref("disk.img"): _Content(b"disk"), _ref("ok.txt"): _Content(b"ok")},
        ),
    )

    result = _export(tmp_path, global_flags=("-q",))

    assert result.exit_code == 0, result.output
    assert result.stderr.splitlines() == ["incomplete disk.img: 1 of 2 parts are missing"]
    assert (tmp_path / "out" / "disk.img").read_bytes() == b"disk"


def test_an_item_without_content_is_skipped_and_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _provider({_ref(): [_file("ok.txt"), _file("hollow.txt")]}, {"ok.txt": _Content(b"ok")})
    _install(monkeypatch, provider)

    result = _export(tmp_path)

    assert result.exit_code == 1
    assert (tmp_path / "out" / "ok.txt").exists() and not (tmp_path / "out" / "hollow.txt").exists()
    assert "skipped hollow.txt: it has no content to export" in result.stderr


def test_existing_destinations_stop_the_export_before_anything_is_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, _two_files())
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "top.txt").write_bytes(b"mine")

    result = _export(tmp_path)

    assert result.exit_code == 1
    # Rich may hard-wrap a long path mid-token.
    message = "".join(result.stderr.split())
    assert message.startswith("error:1of2destinationfilesalreadyexists,startingwith")
    assert message.endswith("top.txt—pass--forcetooverwriteit")
    assert (tmp_path / "out" / "top.txt").read_bytes() == b"mine"
    assert not (tmp_path / "out" / "d").exists()  # nothing else was written either


def test_force_overwrites_existing_destinations(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, _two_files())
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "top.txt").write_bytes(b"mine")

    result = _export(tmp_path, "--force")

    assert result.exit_code == 0, result.output
    assert (tmp_path / "out" / "top.txt").read_bytes() == b"top"


def test_an_output_that_is_a_file_is_refused_even_with_force(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, _two_files())
    (tmp_path / "out").write_bytes(b"a file")

    result = _export(tmp_path, "--force")

    assert result.exit_code == 1
    assert " ".join(result.stderr.split()).endswith("is a file — exporting a folder needs a directory")
    assert (tmp_path / "out").read_bytes() == b"a file"


def test_a_single_item_cannot_be_exported_onto_a_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    item = RestorableUnit(ref=_ref("x"), name="x", is_leaf=True, content=_Content())

    @faithful_to(Repository)
    class _OneItemRepo:
        async def resolve(self, ref: str | NodeRef, *, raw: RawView | None = None) -> NodeFrame:
            return item_frame(item)

    install_fake_session(monkeypatch, [_OneItemRepo()])
    (tmp_path / "out").mkdir()

    result = _export(tmp_path)

    assert result.exit_code == 1
    assert " ".join(result.stderr.split()).endswith("is a directory — a single item exports to a file path")


def test_a_ref_above_a_backup_version_cannot_be_exported_as_a_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    @faithful_to(Repository)
    class _CatalogRepo(_FakeRepo):
        async def resolve(self, ref: str | NodeRef, *, raw: RawView | None = None) -> NodeFrame:
            # What the real Repository.resolve() raises for a ref stopping above a version.
            raise NotFoundError(
                "human ref must name at least a catalog, workload, and version (got 1 segment)", ref="x#Source"
            )

    provider = _provider({})

    install_fake_session(monkeypatch, [_CatalogRepo(provider.root(), provider)])

    result = _export(tmp_path)

    assert result.exit_code == 1
    assert "must name at least a catalog, workload, and version" in " ".join(result.stderr.split())


def test_a_failure_on_one_item_names_it_and_keeps_the_ones_already_done(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = _provider(
        {_ref(): [_file("a.txt"), _folder("d")], _ref("d"): [_file("d", "b.txt"), _file("d", "c.txt")]},
        {
            "a.txt": _Content(b"a"),
            "d/b.txt": _Content(b"b", DataCorruptError("bad chunk")),
            "d/c.txt": _Content(b"c"),
        },
    )
    _install(monkeypatch, provider)

    result = _export(tmp_path)

    assert result.exit_code == 1
    assert result.stderr == "error: d/b.txt: bad chunk\n"
    assert (tmp_path / "out" / "a.txt").read_bytes() == b"a"
    assert not (tmp_path / "out" / "d" / "b.txt").exists() and not (tmp_path / "out" / "d" / "b.txt.part").exists()
    assert not (tmp_path / "out" / "d" / "c.txt").exists()  # the run stopped at the first failure


def test_running_out_of_room_names_the_file_being_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _provider(
        {_ref(): [_file("a.txt"), _file("full.txt")]},
        {"a.txt": _Content(b"a"), "full.txt": _Content(b"x", OSError(errno.ENOSPC, "No space left on device"))},
    )
    _install(monkeypatch, provider)

    result = _export(tmp_path)

    assert result.exit_code == 1
    message = "".join(result.stderr.split())
    assert message == "".join(
        f"error: not enough free space to write {tmp_path / 'out' / 'full.txt'} (No space left on device) — "
        "partial file removed (use --keep-partial to keep it)".split()
    )
    assert (tmp_path / "out" / "a.txt").exists()


def test_cancelling_reports_how_many_files_were_done(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _provider(
        {_ref(): [_file("a.txt"), _file("b.txt")]},
        {"a.txt": _Content(b"a"), "b.txt": _Content(b"partial", asyncio.CancelledError("cancelled"))},
    )
    _install(monkeypatch, provider)

    result = _export(tmp_path)

    assert result.exit_code == 130, result.output
    assert " ".join(result.stdout.split()) == (
        "cancelled — partial file removed (use --keep-partial to keep it) 1 of 2 files had been exported"
    )
    assert (tmp_path / "out" / "a.txt").exists()
    assert not (tmp_path / "out" / "b.txt").exists() and not (tmp_path / "out" / "b.txt.part").exists()


# -- what is already on disk: directories, files in the way, and case-insensitive names --------


@pytest.mark.parametrize("flags", [(), ("--force",)])
def test_a_directory_where_an_item_should_go_is_refused_cleanly_even_with_force(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flags: tuple[str, ...]
) -> None:
    _install(monkeypatch, _two_files())
    (tmp_path / "out" / "top.txt").mkdir(parents=True)

    result = _export(tmp_path, *flags)

    assert result.exit_code == 1
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "".join(result.stderr.split()).endswith("top.txtisadirectory—anitemcannotreplaceit")
    assert not (tmp_path / "out" / "d").exists()  # nothing was written


@pytest.mark.parametrize("flags", [(), ("--force",)])
def test_a_file_where_a_folder_should_be_is_refused_cleanly_even_with_force(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flags: tuple[str, ...]
) -> None:
    _install(monkeypatch, _two_files())
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "d").write_bytes(b"a file where the folder d goes")

    result = _export(tmp_path, *flags)

    assert result.exit_code == 1
    assert result.exception is None or isinstance(result.exception, SystemExit)
    message = "".join(result.stderr.split())
    assert "isafile,so" in message and message.endswith("f.bincannotbecreatedunderit")
    assert (tmp_path / "out" / "d").read_bytes() == b"a file where the folder d goes"
    assert not (tmp_path / "out" / "top.txt").exists()


def test_two_existing_hard_links_are_replaced_as_two_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The duplicate-path check must not mistake names that share an inode for one path: a staged
    write replaces each name with a new file, so the second item's destination is no longer the first's."""
    provider = _provider({_ref(): [_file("a"), _file("b")]}, {"a": _Content(b"AAA"), "b": _Content(b"BBB")})
    _install(monkeypatch, provider)
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "a").write_bytes(b"OLD")
    try:
        os.link(tmp_path / "out" / "a", tmp_path / "out" / "b")
    except (OSError, NotImplementedError):
        pytest.skip("this filesystem has no hard links")

    result = _export(tmp_path, "--force")

    assert result.exit_code == 0, result.output
    assert (tmp_path / "out" / "a").read_bytes() == b"AAA"
    assert (tmp_path / "out" / "b").read_bytes() == b"BBB"


def _dangling_symlink(path: Path) -> None:
    try:
        os.symlink(path.parent / "nowhere", path)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are not available here")


def test_a_dangling_symlink_where_an_item_goes_is_an_existing_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, _two_files())
    (tmp_path / "out").mkdir()
    _dangling_symlink(tmp_path / "out" / "top.txt")

    result = _export(tmp_path)

    assert result.exit_code == 1
    assert "destination files already exist" in " ".join(result.stderr.split())
    assert (tmp_path / "out" / "top.txt").is_symlink()  # not replaced without --force
    assert not (tmp_path / "out" / "d").exists()


def test_a_dangling_symlink_where_a_folder_goes_is_refused_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, _two_files())
    (tmp_path / "out").mkdir()
    _dangling_symlink(tmp_path / "out" / "d")

    result = _export(tmp_path, "--force")

    assert result.exit_code == 1
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "is a file, so" in " ".join(result.stderr.split())
    assert not (tmp_path / "out" / "top.txt").exists()


def _on_a_case_insensitive_filesystem(monkeypatch: pytest.MonkeyPatch, output: Path) -> None:
    """Makes the export see ``output`` as a case-insensitive directory: every path under it is looked up and
    written in lower case, as on the default macOS and Windows filesystems."""
    from synology_apm_repo.sdk.api.export_tree import _file_identity
    from synology_apm_repo.sdk.export import LocalFileSink
    from synology_apm_repo.sdk.presentation.export_target import destination_state

    def fold(path: Path) -> Path:
        try:
            return output / str(path.relative_to(output)).lower()
        except ValueError:
            return path

    class _FoldingSink(LocalFileSink):
        def __init__(self, dst: Path, **kwargs: Any) -> None:
            super().__init__(fold(dst), **kwargs)

    def folded_state(path: Path) -> Literal["missing", "file", "directory"]:
        return destination_state(fold(path))

    def folded_identity(path: Path) -> tuple[int, int] | None:
        return _file_identity(fold(path))

    # The folder export's logic runs in the SDK; the CLI only checks the output folder itself.
    for module in ("synology_apm_repo.cli.commands.export", "synology_apm_repo.sdk.api.export_tree"):
        monkeypatch.setattr(f"{module}.destination_state", folded_state)
    monkeypatch.setattr("synology_apm_repo.sdk.api.export_tree.LocalFileSink", _FoldingSink)
    monkeypatch.setattr("synology_apm_repo.sdk.api.export_tree._file_identity", folded_identity)


def test_names_that_differ_only_in_case_never_overwrite_each_other(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = _provider(
        {_ref(): [_file("one", name="Foo"), _file("two", name="foo")]},
        {"one": _Content(b"FIRST"), "two": _Content(b"second!")},
    )
    _install(monkeypatch, provider)
    _on_a_case_insensitive_filesystem(monkeypatch, tmp_path / "out")

    result = _export(tmp_path)

    assert result.exit_code == 1
    assert (tmp_path / "out" / "foo").read_bytes() == b"FIRST"  # the first one was not replaced
    assert result.stderr.splitlines()[0] == "skipped foo: another item already takes this path"
    assert " ".join(result.stdout.split()).startswith("exported 1 file to ")


def test_force_does_not_let_case_variant_names_replace_each_other_either(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both names resolve to the one file that already exists, so "it existed before" cannot tell them apart."""
    provider = _provider(
        {_ref(): [_file("one", name="Foo"), _file("two", name="foo")]},
        {"one": _Content(b"FIRST"), "two": _Content(b"second!")},
    )
    _install(monkeypatch, provider)
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "foo").write_bytes(b"OLD")
    _on_a_case_insensitive_filesystem(monkeypatch, tmp_path / "out")

    result = _export(tmp_path, "--force")

    assert result.exit_code == 1
    assert (tmp_path / "out" / "foo").read_bytes() == b"FIRST"  # replaced once, by the first item only
    assert result.stderr.splitlines()[0] == "skipped foo: another item already takes this path"


def test_a_folder_and_a_file_whose_names_differ_only_in_case_do_not_clash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = _provider(
        {_ref(): [_folder("A"), _file("a")], _ref("A"): [_file("A", "inside.txt")]},
        {"A/inside.txt": _Content(b"inside"), "a": _Content(b"file")},
    )
    _install(monkeypatch, provider)
    _on_a_case_insensitive_filesystem(monkeypatch, tmp_path / "out")

    result = _export(tmp_path)

    assert result.exit_code == 1
    assert (tmp_path / "out" / "a" / "inside.txt").read_bytes() == b"inside"
    assert result.stderr.splitlines()[0] == "skipped a: another item already takes this path"


def test_a_file_then_a_folder_whose_names_differ_only_in_case_skips_what_is_under_the_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = _provider(
        {_ref(): [_file("a"), _folder("A")], _ref("A"): [_file("A", "inside.txt")]},
        {"A/inside.txt": _Content(b"inside"), "a": _Content(b"file")},
    )
    _install(monkeypatch, provider)
    _on_a_case_insensitive_filesystem(monkeypatch, tmp_path / "out")

    result = _export(tmp_path)

    assert result.exit_code == 1
    assert (tmp_path / "out" / "a").read_bytes() == b"file"
    # WindowsPath compares case-insensitively, so there the plan already skips the folder itself.
    skipped = "A" if sys.platform == "win32" else "A/inside.txt"
    assert result.stderr.splitlines()[0] == f"skipped {skipped}: another item already takes this path"


# -- a failure while opening an item is about that item, not the previous one --------------------


@faithful_to(UnitProvider)
class _FailingOpenProvider(TreeProvider):
    """``unit()`` for ``b.txt`` raises ``error``."""

    def __init__(self, error: BaseException, *args: Any) -> None:
        super().__init__(*args)
        self._error = error

    async def unit(self, node: Node) -> RestorableUnit:
        if node.name == "b.txt":
            raise self._error
        return await super().unit(node)


def _failing_open(error: BaseException) -> _FailingOpenProvider:
    return _FailingOpenProvider(
        error,
        _folder(),
        {_ref(): [_file("a.txt"), _file("b.txt")]},
        {_ref("a.txt"): _Content(b"a")},
    )


def test_an_error_while_opening_an_item_names_that_item(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, _failing_open(DataCorruptError("cannot open b")))

    result = _export(tmp_path)

    assert result.exit_code == 1
    assert result.stderr == "error: b.txt: cannot open b\n"


def test_a_cancel_while_opening_an_item_says_nothing_was_written_for_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, _failing_open(asyncio.CancelledError("cancelled")))

    result = _export(tmp_path)

    assert result.exit_code == 130, result.output
    assert " ".join(result.stdout.split()) == (
        "cancelled — no output file was ever written 1 of 2 files had been exported"
    )
