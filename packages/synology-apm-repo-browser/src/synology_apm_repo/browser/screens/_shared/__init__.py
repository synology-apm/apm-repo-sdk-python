"""Helpers shared by the ``screens`` package, private to it: ``tree_nav.py``
(``Tree`` cursor helpers), ``filter.py`` (the ``/`` filter box),
``goto_ref.py`` (``g`` parsing and resolution), ``errors.py``,
``key_delegation.py`` (action forwarding), ``css.py`` (modal-dialog CSS),
``breadcrumb.py``, ``app_state.py`` (``AppStateMixin``),
``navigable_screen.py`` (``NavigableScreen``) and ``store_screen.py``
(``StoreScreen``, a ``NavigableScreen`` backed by a ``Store``).
"""

from __future__ import annotations

from .app_state import AppStateMixin
from .breadcrumb import breadcrumb_with_tasks_hint
from .css import modal_box_css
from .errors import notify_warning, show_error
from .filter import FilterFieldController, close_filter_debounce, show_filter_input
from .goto_ref import parse_canonical_ref, resolve_and_open_goto_target, resolve_goto_version
from .key_delegation import DelegatesCommonActions, delegate_common_action, forward_to_focused
from .navigable_screen import NavigableScreen
from .store_screen import StoreScreen
from .tree_nav import current_listing_tree_node, move_cursor_to_parent

__all__ = [
    "AppStateMixin",
    "DelegatesCommonActions",
    "FilterFieldController",
    "NavigableScreen",
    "StoreScreen",
    "breadcrumb_with_tasks_hint",
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
