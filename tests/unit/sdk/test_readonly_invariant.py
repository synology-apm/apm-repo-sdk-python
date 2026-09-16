"""Regression test for the "read-only, no network" invariant.

Uses ``sys.addaudithook`` to catch any writable-mode ``open()``/``os.open()``,
any filesystem mutation (``os.remove``/``rename``/``replace``/``mkdir``/
``rmdir``, ``shutil.rmtree``/``move``/``copytree``) under the tested
repository root, and any socket activity at all, during SDK calls. CPython
audit hooks cannot be removed once installed, so a single hook is installed
once (module-level) and each test brackets the region it cares about with
``watch``, inspecting only events logged *within* that bracket rather
than relying on a fresh hook per test.
"""

from __future__ import annotations

import contextlib
import os
import socket
import sys
from collections.abc import Callable, Iterator
from pathlib import Path

from synology_apm_repo.sdk.storage import LocalFsStore, iter_layouts

_WRITE_FLAG_BITS = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC | getattr(os, "O_EXCL", 0)

#: Real CPython audit events a filesystem *mutation* fires -- deliberately excludes
#: read-only events some of these also emit internally (e.g.
#: shutil.rmtree's own "os.scandir").
_MUTATION_EVENTS = frozenset(
    {"os.remove", "os.rename", "os.mkdir", "os.rmdir", "shutil.rmtree", "shutil.move", "shutil.copytree"}
)

_AUDIT_LOG: list[tuple[str, tuple[object, ...]]] = []
_installed = False


def _is_write_open(mode: object, flags: object) -> bool:
    if isinstance(mode, str):
        return any(c in mode for c in "wax+")
    if isinstance(flags, int):
        return bool(flags & _WRITE_FLAG_BITS)
    return False


def _hook(event: str, args: tuple[object, ...]) -> None:
    if event == "open":
        path, mode, flags = args
        if _is_write_open(mode, flags):
            _AUDIT_LOG.append((event, args))
    elif event.startswith("socket.") or event in _MUTATION_EVENTS:
        _AUDIT_LOG.append((event, args))


def _ensure_hook_installed() -> None:
    global _installed
    if not _installed:
        sys.addaudithook(_hook)
        _installed = True


@contextlib.contextmanager
def watch(*, under: Path | None = None) -> Iterator[Callable[[], list[tuple[str, tuple[object, ...]]]]]:
    """Bracket a region of code; the yielded callable returns violations
    logged strictly within that bracket. Writable-``open`` events are
    filtered to those whose path falls under ``under`` (when given);
    socket events are never filtered — there should be zero of them,
    period, during offline SDK operation.
    """
    _ensure_hook_installed()
    start = len(_AUDIT_LOG)

    def violations() -> list[tuple[str, tuple[object, ...]]]:
        entries = _AUDIT_LOG[start:]
        if under is None:
            return list(entries)
        under_str = str(under)
        return [(event, args) for event, args in entries if event.startswith("socket.") or under_str in str(args[0])]

    yield violations


def _make_minimal_repo(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "repo_info").write_bytes(b"RpiF" + b"\x00" * 60)
    (root / "link.key").write_bytes(b"\x00" * 64)
    (root / ".fully_created").write_bytes(b"")
    (root / "db").mkdir()
    (root / "@data").mkdir()


async def test_layout_discovery_and_reads_never_write_or_touch_network(tmp_path: Path) -> None:
    _make_minimal_repo(tmp_path)  # setup writes happen *before* the watch bracket

    with watch(under=tmp_path) as violations:
        store = LocalFsStore(tmp_path)
        [layout async for layout in iter_layouts(store)]
        await store.read("repo_info")
        await store.size("link.key")
        await store.exists("db")
        await store.listdir("")

        assert violations() == []


async def test_reading_a_missing_path_also_stays_read_only(tmp_path: Path) -> None:
    _make_minimal_repo(tmp_path)

    with watch(under=tmp_path) as violations:
        store = LocalFsStore(tmp_path)
        for _ in range(3):
            # deliberately swallow NotFoundError — we only care about I/O side effects here
            with contextlib.suppress(Exception):
                await store.read("does/not/exist")

        assert violations() == []


async def test_watch_catches_a_writable_open_under_the_root(tmp_path: Path) -> None:
    """Prove the hook itself works: a deliberate write inside a ``watch()``
    bracket must show up as a violation, not just be permitted by omission
    in every other test's happy path.
    """
    _make_minimal_repo(tmp_path)
    target = tmp_path / "x"

    with watch(under=tmp_path) as violations:
        # Deliberately the blocking builtin open() (not os.open, covered by the
        # sibling test below) -- this is the branch of the hook that reads a
        # string mode rather than integer flags.
        with open(target, "w") as f:  # noqa: ASYNC230
            f.write("this write must be flagged")

        found = violations()

    assert found != []
    (event, args) = found[0]
    assert event == "open"
    path, mode, _flags = args
    assert str(target) in str(path)
    assert _is_write_open(mode, _flags)


async def test_watch_catches_os_open_with_a_write_flag(tmp_path: Path) -> None:
    _make_minimal_repo(tmp_path)
    target = tmp_path / "y"

    with watch(under=tmp_path) as violations:
        fd = os.open(target, os.O_WRONLY | os.O_CREAT)
        os.close(fd)

        found = violations()

    assert found != []
    (event, args) = found[0]
    assert event == "open"
    path, _mode, flags = args
    assert str(target) in str(path)
    assert _is_write_open(_mode, flags)


async def test_watch_catches_os_remove_and_os_rename_under_the_root(tmp_path: Path) -> None:
    """Prove the mutation-event branch of the hook works: the writable-``open``
    branch alone would never catch a deletion/rename that touches an
    *already-existing* file through no ``open()`` call at all."""
    _make_minimal_repo(tmp_path)
    src = tmp_path / "a.txt"
    dst = tmp_path / "b.txt"
    src.write_bytes(b"x")  # setup write happens *before* the watch bracket

    with watch(under=tmp_path) as violations:
        os.rename(src, dst)
        os.remove(dst)

        found = violations()

    events = [event for event, _args in found]
    assert events == ["os.rename", "os.remove"]


async def test_watch_catches_socket_creation(tmp_path: Path) -> None:
    """Prove the ``socket.*`` branch of the hook works: creating a real
    socket inside a ``watch()`` bracket must be flagged, unconditionally
    (socket violations are never filtered by ``under``).
    """
    with watch(under=tmp_path) as violations:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            found = violations()
        finally:
            s.close()

    assert found != []
    assert all(event.startswith("socket.") for event, _args in found)
