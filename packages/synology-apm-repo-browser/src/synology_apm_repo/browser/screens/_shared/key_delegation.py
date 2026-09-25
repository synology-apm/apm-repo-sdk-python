"""Shared key/action-delegation helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from textual.screen import Screen

if TYPE_CHECKING:
    from synology_apm_repo.browser.app import ApmRepoBrowserApp


def forward_to_focused(screen: Screen[Any], action_name: str) -> None:
    """Forwards ``action_name`` to whichever widget currently has focus,
    if it implements that action (``DataTable``/``Tree`` both implement
    ``action_cursor_down``/``_up``/``action_select_cursor``; anything
    without it — an ``Input``, say — simply ignores the forward). Shared
    by ``NavigableScreen``'s own ``action_cursor_down``/``_up``/
    ``action_select`` and ``WorklistScreen``'s (a ``ModalScreen``, so it
    can't inherit ``NavigableScreen`` itself, but still hosts a
    ``DataTable`` with the same ``j``/``k`` forwarding need) rather than
    each keeping its own identical copy."""
    action = getattr(screen.focused, action_name, None)
    if callable(action):
        action()


def delegate_common_action(screen: Screen[Any], action_name: str) -> None:
    """Runs ``action_<action_name>`` on the App directly — the fix a
    ``ModalScreen`` keeping ``COMMON_BINDINGS`` in its own ``BINDINGS``
    needs: Textual's own action dispatch runs the method on whichever node the
    key's own ``Binding`` was found on, never bubbling further once the
    modal chain is truncated at that screen, so each of ``quit_app``/
    ``toggle_verbose``/``show_help`` still needs its own same-named
    ``action_*`` method on the screen to redirect through here, rather
    than dispatch ever reaching the App on its own."""
    app = cast("ApmRepoBrowserApp", screen.app)
    getattr(app, f"action_{action_name}")()
