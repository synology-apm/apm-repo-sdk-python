import datetime
import os
import sys
import tomllib
from pathlib import Path

sys.path.insert(0, os.path.abspath("../packages/synology-apm-repo-sdk/src"))

project = "Synology APM Repository SDK"
copyright = f"{datetime.date.today().year}, Synology Inc."
author = "Synology Inc."
with open(Path(__file__).parent.parent / "packages/synology-apm-repo-sdk/pyproject.toml", "rb") as f:
    _pkg_version = tomllib.load(f)["project"]["version"]

version = ".".join(_pkg_version.split(".")[:2])
release = _pkg_version

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
]

autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
    "member-order": "bysource",
    # False: True would pull in stdlib base classes' own docstrings (e.g.
    # dict.get's "D[k] if k in D, else d."), whose "<words>: text" first
    # lines Napoleon misreads as type names.
    "inherited-members": False,
}
autodoc_typehints = "description"
autodoc_typehints_format = "short"
autodoc_typehints_description_target = "documented"

napoleon_google_docstring = True
napoleon_numpy_docstring = False
# Use :ivar: for the Attributes section; prevents duplicate entries with
# autodoc's dataclass field discovery.
napoleon_use_ivar = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

nitpick_ignore = [
    # The async-context-manager mixins behind `async with`; _util gets no
    # apidoc page, so show-inheritance's "Bases:" link has no target.
    ("py:class", "synology_apm_repo.sdk._util.closing.AsyncClosing"),
    # PEP 695 type parameters (`run_sink_export[T]`, `CacheManager.keyed[K, V]`,
    # `SaasWorkloadConfig[StateT]`): Sphinx renders each as an unresolved
    # class reference, and with postponed annotations earlier 3.12 patch
    # releases (CI's 3.12.3) leave even `T` an unresolved string.
    *(("py:class", name) for name in ("T", "K", "V", "StateT")),
    # The same 3.12.3 postponed-annotation gap for SaasWorkloadConfig's own
    # field types, tree_strategy's ``Row``/``Key`` aliases and ``TreeStrategy``;
    # absent on 3.12.13, so ignored only below it.
    *(("py:class", name) for name in ("Row", "Key", "TreeStrategy") if sys.version_info < (3, 12, 13)),
    # aiosqlite, zstandard and cryptography ship no Sphinx inventory for intersphinx.
    ("py:class", "aiosqlite.core.Connection"),
    ("py:class", "cryptography.hazmat.primitives.ciphers.algorithms.AES"),
    ("py:exc", "zstandard.ZstdError"),
    # __module__ is the private asyncio.locks; docs.python.org's inventory
    # indexes only the public asyncio.Semaphore path.
    ("py:class", "asyncio.locks.Semaphore"),
    # __module__ is the private _base leaf (no apidoc page); the class is
    # re-exported from units.saas.tree_strategy.
    ("py:class", "synology_apm_repo.sdk.units.saas.tree_strategy._base.TreeStrategy"),
]

html_theme = "furo"
html_title = "Synology APM Repository SDK"
html_show_sphinx = False
html_show_sourcelink = False
html_copy_source = False

exclude_patterns = [
    "_build",
    "api/synology_apm_repo.rst",  # namespace package root, nothing to document
    # Pure re-export __init__.py files: a page would repeat what each
    # symbol's own submodule page already documents.
    "api/synology_apm_repo.sdk.rst",
    "api/synology_apm_repo.sdk.api.rst",
    "api/synology_apm_repo.sdk.export.rst",
    "api/synology_apm_repo.sdk.presentation.rst",
    "api/synology_apm_repo.sdk.storage.rst",
    "api/synology_apm_repo.sdk.units.saas.tree_strategy.rst",
    # Docstring-only __init__.py files: ARCHITECTURE.md covers each layer,
    # and apidoc's alphabetical "Submodules" toctree would double-list the
    # submodules index.rst already orders by hand.
    "api/synology_apm_repo.sdk.catalog.rst",
    "api/synology_apm_repo.sdk.dedup.rst",
    "api/synology_apm_repo.sdk.format.rst",
    "api/synology_apm_repo.sdk.units.content.rst",
    # Same, and its toctree would also reference the excluded units.saas page.
    "api/synology_apm_repo.sdk.units.rst",
    # Empty __init__.py: a page would render nothing.
    "api/synology_apm_repo.sdk.units.saas.rst",
]
