"""Shared key/action-delegation helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from textual.screen import Screen

if TYPE_CHECKING:
    from synology_apm_repo.browser.app import ApmRepoBrowserApp


def forward_to_focused(screen: Screen[Any], action_name: str) -> None:
    """Runs ``action_name`` on the focused widget if it has that action
    (``DataTable``/``Tree`` do; an ``Input`` doesn't, and ignores it)."""
    action = getattr(screen.focused, action_name, None)
    if callable(action):
        action()


def delegate_common_action(screen: Screen[Any], action_name: str) -> None:
    """Runs ``action_<action_name>`` on the App. A ``ModalScreen`` truncates
    the binding chain at itself and Textual runs a binding's action on the
    node that declares it, so a modal keeping ``COMMON_BINDINGS`` needs a
    same-named ``action_*`` method that redirects here."""
    app = cast("ApmRepoBrowserApp", screen.app)
    getattr(app, f"action_{action_name}")()


class DelegatesCommonActions:
    """Mixed into a ``ModalScreen`` that keeps ``COMMON_BINDINGS``: the
    same-named ``action_*`` methods ``delegate_common_action`` needs on it."""

    def action_quit_app(self) -> None:
        delegate_common_action(cast("Screen[Any]", self), "quit_app")

    def action_toggle_verbose(self) -> None:
        delegate_common_action(cast("Screen[Any]", self), "toggle_verbose")

    def action_show_help(self) -> None:
        delegate_common_action(cast("Screen[Any]", self), "show_help")
