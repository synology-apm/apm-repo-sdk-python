"""``KeyDialog``: a small, centered modal overlay — not a full-screen
view — for pasting a ``<userKeyID>@<base64(userKey)>`` key string.

For why ``BrowseScreen`` pushes this dialog automatically with no
keybinding reaching it, see its ``_prompt_for_key`` (``browse_screen.py``),
triggered by ``KeyRequiredError``/``KeyMismatchError`` from ``catalog.workloads()``.

Dismisses with ``True`` (a key was verified) or ``False`` (cancelled, or
every attempt so far failed) — ``BrowseScreen`` decides what to do with
either outcome (proceed to load the blocked workload list, or leave it
blocked and the connection effectively un-entered).
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Input, Static

from synology_apm_repo.browser.screens._shared import modal_box_css, notify_warning, show_error
from synology_apm_repo.browser.strings import KEY_INPUT_PLACEHOLDER, KEY_PROMPT
from synology_apm_repo.browser.widgets.progress_hint import StaticTextSink
from synology_apm_repo.browser.widgets.worker_progress import work
from synology_apm_repo.sdk.api import Repository
from synology_apm_repo.sdk.errors import ApmRepoError


class KeyDialog(ModalScreen[bool]):
    """``ModalScreen`` truncates the App-level binding chain at itself
    (Textual's own design: a modal's own
    bindings take precedence over, and hide, everything below it — see
    ``textual.screen.Screen._modal_binding_chain``) — there is
    deliberately no ``d``/``q``/``?`` reachable while this dialog is open,
    only the two actions it actually needs, declared directly here
    rather than inherited from ``COMMON_BINDINGS``."""

    DEFAULT_CSS = (
        modal_box_css("KeyDialog", width=64)
        + """
    KeyDialog #key-status {
        margin-top: 1;
    }
    """
    )

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=False),
        Binding("enter", "verify", "Verify", show=False),
    ]

    def __init__(self, repo: Repository) -> None:
        super().__init__()
        self._repo = repo

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

    # No busy feedback here at all before this -- `set_key()` is real SDK I/O
    # with no size/latency guarantee, and #key-status starts empty, so an
    # empty base text is what the debounced sink falls back to.
    @work(sink=lambda self, key_string: StaticTextSink(self, "#key-status", base=lambda: ""))
    async def _verify(self, key_string: str) -> None:
        status = self.query_one("#key-status", Static)
        try:
            verification = await self._repo.set_key(key_string)
        except ApmRepoError as exc:
            show_error(self, "#key-status", exc)
            return
        except ExceptionGroup as exc:
            # Repository.set_key() raises ExceptionGroup (not an
            # ApmRepoError) only when the key itself verified fine but
            # reopening/closing one specific already-opened sibling
            # catalog independently failed — self._repo.key_status is
            # already VERIFIED at this point -- that update happens
            # *before* this exception is raised, so
            # this is a genuinely accepted key with a partial cleanup
            # failure alongside it, not a rejected one. Notify about the
            # failure (so it isn't silently lost) but still proceed with
            # the verified key below, rather than looping the user back
            # to re-enter a key that already worked — showing an error
            # and refusing to dismiss here would desync this dialog from
            # a Repository that already considers the key valid.
            notify_warning(self, exc)
            maybe_verification = self._repo.key_verification
            assert maybe_verification is not None  # set_key() always sets this before raising
            verification = maybe_verification
        color = "green" if verification.ok else "red"
        text = (
            f"[{color}]{'verified' if verification.ok else 'invalid'}[/{color}] "
            f"gcm_ok={verification.gcm_ok}\n"
            "Press Enter to retry with a different key, or Esc to cancel."
        )
        status.update(text)
        if verification.ok:
            self.dismiss(True)
