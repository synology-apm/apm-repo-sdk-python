"""``KeyDialog``: a centered modal for pasting a
``<userKeyID>@<base64(userKey)>`` key string. ``BrowseScreen`` pushes it
automatically (see ``BrowseEffects._prompt_for_key``); no keybinding reaches it.

Dismisses with ``True`` (a key was verified) or ``False`` (cancelled with Esc).
"""

from __future__ import annotations

from typing import ClassVar, override

from textual.app import ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Input, Static

from synology_apm_repo.browser.screens._shared import modal_box_css, show_error
from synology_apm_repo.browser.strings import KEY_INPUT_PLACEHOLDER, KEY_PROMPT
from synology_apm_repo.browser.widgets.progress_hint import StaticTextSink
from synology_apm_repo.browser.widgets.worker_progress import work
from synology_apm_repo.sdk import ApmRepoError, Repository


class KeyDialog(ModalScreen[bool]):
    """Keeps no ``COMMON_BINDINGS``: the App's ``q``/``d``/``?`` don't reach
    past a modal."""

    DEFAULT_CSS = (
        modal_box_css("KeyDialog", width=64)
        + """
    KeyDialog #key-status {
        margin-top: 1;
    }
    """
    )

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("escape", "cancel", "Cancel", show=False),
        Binding("enter", "verify", "Verify", show=False),
    ]

    def __init__(self, repo: Repository) -> None:
        super().__init__()
        self._repo = repo

    @override
    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(KEY_PROMPT, id="key-dialog-prompt")
            yield Input(placeholder=KEY_INPUT_PLACEHOLDER, password=True, id="key-input")
            yield Static("", id="key-status")

    def on_mount(self) -> None:
        self.query_one("#key-input", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "key-input" and event.value:
            self._verify(event.value)

    def action_verify(self) -> None:
        value = self.query_one("#key-input", Input).value
        if value:
            self._verify(value)

    def action_cancel(self) -> None:
        self.dismiss(False)

    # set_key() is real I/O; #key-status starts empty, so the debounced sink's
    # base text is "".
    @work(sink=lambda self, key_string: StaticTextSink(self, "#key-status", base=lambda: ""))
    async def _verify(self, key_string: str) -> None:
        status = self.query_one("#key-status", Static)
        try:
            result = await self._repo.set_key(key_string)
        except ApmRepoError as exc:
            show_error(self, "#key-status", exc)
            return
        if result.warning is not None:
            # The key verified but a sibling catalog failed to reopen/close:
            # notify, and still proceed with the verified key.
            self.notify(result.warning, severity="warning")
        verification = result.verification
        color = "green" if verification.ok else "red"
        text = (
            f"[{color}]{'verified' if verification.ok else 'invalid'}[/{color}] "
            f"gcm_ok={verification.gcm_ok}\n"
            "Press Enter to retry with a different key, or Esc to cancel."
        )
        status.update(text)
        if verification.ok:
            self.dismiss(True)
