"""``ReleasesOnPrune``: a screen mixin that stops its workers and store
subscriptions once Textual starts removing it."""

from __future__ import annotations

from textual.dom import DOMNode
from textual.messages import Prune


class ReleasesOnPrune(DOMNode):
    """Mixed into a screen whose workers or store subscriptions write to
    its widgets: when Textual starts removing the screen, before any of its
    widgets go (``on_unmount`` comes only after), cancels the screen's own
    workers, then runs ``_release_on_prune``."""

    def _on_prune(self, _event: Prune) -> None:
        self.workers.cancel_node(self)
        self._release_on_prune()

    def _release_on_prune(self) -> None:
        """Drops anything else that renders into this screen (a store's
        subscriptions); nothing by default."""
