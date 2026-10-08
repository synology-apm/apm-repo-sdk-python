"""Load a repository script or example (neither is an installed package) by path."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def load_module(name: str, path: Path) -> ModuleType:
    """A fresh execution of the file at ``path`` as module ``name``.

    It is registered in ``sys.modules`` before executing, as an import would
    be, so that ``dataclasses`` and ``typing`` can resolve the module's own
    names; each call replaces the previous registration.
    """
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
