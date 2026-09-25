"""Shared base class for screens whose primary widget is a
``DataTable``/``Tree`` (vim-style ``j``/``k``/``l``/Enter bindings, kept
generic here instead of re-implemented per screen) —
private to the ``screens`` package (leading underscore), not part of the
browser's own public surface.

Split by concern into sibling modules: ``tree_nav.py`` (``Tree`` cursor
helpers), ``filter.py`` (the shared filter-box mechanics/
``FilterFieldController``), ``goto_ref.py`` (canonical-ref goto parsing/
resolution), ``errors.py`` (error/warning display), ``key_delegation.py``
(action forwarding/delegation), ``css.py`` (the shared modal-dialog CSS
builder), ``breadcrumb.py`` (the breadcrumb-text builder), and
``navigable_screen.py`` (``NavigableScreen`` itself, composed from
several of the above).
"""

from __future__ import annotations

from .breadcrumb import _breadcrumb_with_tasks_hint
from .css import modal_box_css
from .errors import notify_warning, show_error
from .filter import FilterFieldController, close_filter_debounce, show_filter_input
from .goto_ref import parse_canonical_ref, resolve_and_open_goto_target, resolve_goto_version
from .key_delegation import delegate_common_action, forward_to_focused
from .navigable_screen import NavigableScreen
from .tree_nav import current_listing_tree_node, move_cursor_to_parent

__all__ = [
    "FilterFieldController",
    "NavigableScreen",
    "_breadcrumb_with_tasks_hint",
    "close_filter_debounce",
    "current_listing_tree_node",
    "delegate_common_action",
    "forward_to_focused",
    "modal_box_css",
    "move_cursor_to_parent",
    "notify_warning",
    "parse_canonical_ref",
    "resolve_and_open_goto_target",
    "resolve_goto_version",
    "show_error",
    "show_filter_input",
]
