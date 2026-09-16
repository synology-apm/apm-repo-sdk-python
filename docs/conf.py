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
    # Deliberately False: every base class here is a stdlib type
    # (Enum/Exception/Protocol/NamedTuple/Mapping), so True pulls in their
    # own C-implemented docstrings verbatim (e.g. dict.get's "D[k] if k in
    # D, else d.  d defaults to None."), and Napoleon's property/attribute
    # type-shorthand parsing (a bare "<words>: description" first line)
    # misreads that prose as a type name with no docstring on our side able
    # to fix it — the text comes from CPython itself.
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
    # AsyncKeyedCache's own type parameters, not real linkable objects.
    ("py:obj", "synology_apm_repo.sdk.asynccache.K"),
    ("py:obj", "synology_apm_repo.sdk.asynccache.V"),
    # aiosqlite ships no Sphinx inventory to add to intersphinx_mapping.
    ("py:class", "aiosqlite.core.Connection"),
    # asyncio.Semaphore's own __module__ is the private asyncio.locks --
    # CPython's docs.python.org inventory indexes it under the public
    # asyncio.Semaphore path instead, so intersphinx can't resolve the
    # fully-qualified name autodoc emits.
    ("py:class", "asyncio.locks.Semaphore"),
]

html_theme = "furo"
html_title = "Synology APM Repository SDK"
html_show_sphinx = False
html_show_sourcelink = False
html_copy_source = False

exclude_patterns = [
    "_build",
    "api/synology_apm_repo.rst",  # namespace package root, nothing to document
    # These __init__.py files are pure re-exports (docstring + imports + __all__,
    # no functions/classes of their own) — an automodule page for them would just
    # repeat what's already documented on the submodule each symbol comes from.
    "api/synology_apm_repo.sdk.rst",
    "api/synology_apm_repo.sdk.api.rst",
    "api/synology_apm_repo.sdk.storage.rst",
    # These __init__.py files hold only a module docstring (no imports, no
    # functions/classes) — the docstring itself is real, but its content is
    # already covered at more length by ARCHITECTURE.md's own per-layer
    # section, and including the page would pull in sphinx-apidoc's own
    # generated "Submodules" toctree, which would double-list every
    # submodule below in apidoc's alphabetical order, right next to this
    # same package's own deliberately-ordered (not alphabetical) flat list.
    "api/synology_apm_repo.sdk.catalog.rst",
    "api/synology_apm_repo.sdk.dedup.rst",
    "api/synology_apm_repo.sdk.format.rst",
    "api/synology_apm_repo.sdk.units.content.rst",
    # Same reasoning as the block above, plus a second, independent reason:
    # its own generated toctree also references
    # api/synology_apm_repo.sdk.units.saas.rst, which stays excluded below
    # (a genuinely empty __init__.py) — including this page would raise
    # its own "toctree contains reference to excluded document" warning.
    "api/synology_apm_repo.sdk.units.rst",
    # Genuinely empty __init__.py (0 bytes: no docstring, no imports, no
    # functions/classes) — an automodule page would render nothing at all.
    "api/synology_apm_repo.sdk.presentation.rst",
    "api/synology_apm_repo.sdk.units.saas.rst",
]
