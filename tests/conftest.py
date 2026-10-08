"""Session-wide setup for every test: a fixed local timezone, a Rich that
sees no terminal and 80 columns, the hypothesis settings profiles, and the
``pilot`` marker on every Textual ``Pilot`` test. Narrower fixtures live in
the ``conftest.py`` of the directory that uses them."""

from __future__ import annotations

import ast
import os
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone, tzinfo
from functools import cache
from pathlib import Path
from typing import Self

import pytest
from hypothesis import settings
from rich.console import detect_legacy_windows

# ``HYPOTHESIS_PROFILE`` picks one: "default" keeps the local suite fast, CI
# runs "ci". No deadline: a busy xdist worker's wall clock says nothing about
# the property.
settings.register_profile("default", max_examples=50, deadline=None)
settings.register_profile("ci", max_examples=200, deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))

_FIXED_TZ = "Asia/Taipei"

#: Asia/Taipei has observed no DST since 1979, so a bare +08:00 renders every
#: backup timestamp exactly as the named zone does — and unlike
#: ``ZoneInfo(_FIXED_TZ)`` it needs no ``tzdata``, which Windows ships no system
#: copy of and this workspace only pulls in transitively.
_FIXED_TZ_OFFSET = timezone(timedelta(hours=8))


class _FixedLocalDatetime(datetime):
    """``datetime`` whose zone-less ``fromtimestamp()`` returns
    ``_FIXED_TZ_OFFSET`` wall time rather than the running machine's — see
    ``_fixed_timezone``."""

    @classmethod
    def fromtimestamp(cls, t: float, tz: tzinfo | None = None) -> Self:
        if tz is None:
            return super().fromtimestamp(t, _FIXED_TZ_OFFSET).replace(tzinfo=None)
        return super().fromtimestamp(t, tz)


@pytest.fixture(scope="session", autouse=True)
def _fixed_timezone() -> Iterator[None]:
    """Pins local-time rendering to Asia/Taipei for the whole run, so an
    assertion on a ``format_timestamp`` result reproduces on any machine.

    Where ``time.tzset()`` exists, ``TZ`` pins every local-time call in the
    process. Windows has no ``tzset`` and its CRT ignores ``TZ``, so there the
    ``datetime`` of ``presentation/format.py``, the SDK's one local-time
    conversion, is patched instead.
    """
    if hasattr(time, "tzset"):
        previous = os.environ.get("TZ")
        os.environ["TZ"] = _FIXED_TZ
        time.tzset()
        try:
            yield
        finally:
            if previous is None:
                del os.environ["TZ"]
            else:
                os.environ["TZ"] = previous
            time.tzset()
    else:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr("synology_apm_repo.sdk.presentation.format.datetime", _FixedLocalDatetime)
            yield


def pytest_configure(config: pytest.Config) -> None:
    """Makes ``cli/consoles.py`` see no terminal, 80 columns wide, before
    collection imports it.

    Its module-level ``rich.Console()`` objects resolve ``color_system="auto"``
    and their width once, when constructed, so anything that makes Rich see a
    terminal then puts ANSI into every CLI test asserting on exact rendered
    text: a non-empty ``FORCE_COLOR`` (even ``"0"``), or, in a Windows xdist
    worker, a stdout on the null device, which reports ``isatty()``.
    ``TTY_COMPATIBLE=0`` (Rich 14+) overrides ``isatty()``. An inherited
    ``COLUMNS`` would re-wrap every long line a snapshot holds, and so would
    Rich's legacy-Windows mode (any Windows stdout without VT processing: a
    pipe, a file, an xdist worker's), which takes one column off ``COLUMNS``.
    A fixture runs after collection, too late for any of these.
    """
    os.environ.pop("FORCE_COLOR", None)
    os.environ["TTY_COMPATIBLE"] = "0"
    os.environ["COLUMNS"] = str(80 + detect_legacy_windows())


_TESTS = Path(__file__).parent


@cache
def _drives_a_pilot(path: Path) -> bool:
    """Whether the module at ``path``, or a ``tests/`` module it imports
    from, calls ``App.run_test()``."""
    source = path.read_text(encoding="utf-8")
    if ".run_test(" in source:
        return True
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and node.module and not node.level:
            local = _TESTS.joinpath(*node.module.split(".")).with_suffix(".py")
            if local.is_file() and local != path and _drives_a_pilot(local):
                return True
    return False


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Marks every test in a module that drives a Textual ``Pilot`` with
    ``pilot``, so ``-m "not pilot"`` (``make test-quick``) skips the slow
    ones."""
    for item in items:
        if _drives_a_pilot(item.path):
            item.add_marker(pytest.mark.pilot)
