"""Fixtures scoped to ``tests/integration/browser/`` only -- see
``tests/integration/cli/conftest.py`` for the precedent this follows
(a fixture belonging to one slice of the sdk/cli/browser split, not the
whole suite, stays out of root ``tests/conftest.py``)."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _apply_fast_browser_debounce(fast_browser_debounce: None) -> None:
    """Auto-activates ``tests/conftest.py``'s ``fast_browser_debounce`` for
    every test in this directory. The underlying logic stays in the
    shared root conftest (costing nothing for a test elsewhere that
    never asks for it by name); only its *autouse* activation is scoped
    per directory, by depending on it from an autouse fixture of its
    own here -- pytest resolves it by name up the conftest hierarchy, no
    import needed."""
