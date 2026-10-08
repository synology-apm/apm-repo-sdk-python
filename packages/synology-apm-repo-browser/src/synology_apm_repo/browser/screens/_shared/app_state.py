"""``AppStateMixin``: typed access to the running ``ApmRepoBrowserApp``."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from textual.dom import DOMNode

if TYPE_CHECKING:
    from synology_apm_repo.browser.app import ApmRepoBrowserApp


class AppStateMixin(DOMNode):
    """Mixed into a ``Screen``/``ModalScreen``: ``app_state`` is ``self.app``
    as the ``ApmRepoBrowserApp`` every screen here runs under."""

    @property
    def app_state(self) -> ApmRepoBrowserApp:
        return cast("ApmRepoBrowserApp", self.app)
