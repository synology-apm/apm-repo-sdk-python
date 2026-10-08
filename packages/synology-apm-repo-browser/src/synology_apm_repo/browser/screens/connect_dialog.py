"""``ConnectDialog``: the modal that picks where to browse -- a local
directory or an SMB/S3/Azure store -- opened at launch and by ``c``.

``_scan`` scans the store (``runtime/connect.py``, which also builds it),
streaming progress into ``#connect-status``, and dismisses once at least
one repository was found; validation errors, connection failures and
empty scans show inline. The local tab pairs a ``DirsOnlyTree`` with a path
``Input`` that mirrors its selection.

Two collaborators reach back through the surface exposed for them:
``SavedProfileManager`` (saved profiles) and ``RemoteOptionsBrowser`` (the
bucket/container "Browse" flow).
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import ClassVar, cast, override

from textual.app import ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Horizontal, Vertical
from textual.css.query import NoMatches
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, DirectoryTree, Input, OptionList, Select, Static, Tab, Tabs, Tree
from textual.worker import Worker

from synology_apm_repo.browser.runtime.connect import local_store, remote_store, scan_repositories
from synology_apm_repo.browser.screens._shared import AppStateMixin, modal_box_css, move_cursor_to_parent, show_error
from synology_apm_repo.browser.screens.profile_manager import SavedProfileManager, profile_backend_of
from synology_apm_repo.browser.screens.remote_browser import RemoteOptionsBrowser
from synology_apm_repo.browser.strings import (
    CONNECT_AZURE_ACCOUNT_URL_PLACEHOLDER,
    CONNECT_AZURE_BROWSE_CONTAINERS_LABEL,
    CONNECT_AZURE_CONTAINER_PLACEHOLDER,
    CONNECT_AZURE_CREDENTIAL_PLACEHOLDER,
    CONNECT_AZURE_PROFILE_SELECT_PROMPT,
    CONNECT_BACKEND_AZURE_LABEL,
    CONNECT_BACKEND_LOCAL_LABEL,
    CONNECT_BACKEND_S3_LABEL,
    CONNECT_BACKEND_SMB_LABEL,
    CONNECT_CANCEL_LABEL,
    CONNECT_CANCELLING_STATUS,
    CONNECT_DELETE_PROFILE_LABEL,
    CONNECT_LOCAL_PROMPT,
    CONNECT_PROFILE_NAME_CONFIRM_LABEL,
    CONNECT_PROFILE_NAME_PLACEHOLDER,
    CONNECT_PROMPT,
    CONNECT_S3_ACCESS_KEY_PLACEHOLDER,
    CONNECT_S3_BROWSE_BUCKETS_LABEL,
    CONNECT_S3_BUCKET_PLACEHOLDER,
    CONNECT_S3_ENDPOINT_PLACEHOLDER,
    CONNECT_S3_PROFILE_SELECT_PROMPT,
    CONNECT_S3_REGION_PLACEHOLDER,
    CONNECT_S3_SECRET_KEY_PLACEHOLDER,
    CONNECT_SAVE_PROFILE_LABEL,
    CONNECT_SMB_PASSWORD_PLACEHOLDER,
    CONNECT_SMB_PROFILE_SELECT_PROMPT,
    CONNECT_SMB_SERVER_PLACEHOLDER,
    CONNECT_SMB_SHARE_PLACEHOLDER,
    CONNECT_SMB_USERNAME_PLACEHOLDER,
    CONNECT_SUBMIT_LABEL,
    CONNECT_SUBMIT_LABEL_LOCAL,
    CONNECT_VERIFY_TLS_LABEL,
)
from synology_apm_repo.browser.widgets.dirs_only_tree import DirsOnlyTree
from synology_apm_repo.browser.widgets.worker_progress import work
from synology_apm_repo.sdk import ObjectStore, Repository
from synology_apm_repo.sdk.presentation import Progress, pluralize, safe
from synology_apm_repo.sdk.profiles import DEFAULT_SMB_PORT, BackendKind, account_client_kwargs


class _Backend(enum.StrEnum):
    LOCAL = "local"
    SMB = "smb"
    S3 = "s3"
    AZURE = "azure"


#: Profile widget ids per remote ``BackendKind``.
_SAVE_PROFILE_BUTTON_IDS = tuple(f"connect-{b}-save-profile-button" for b in BackendKind)
_DELETE_PROFILE_BUTTON_IDS = tuple(f"connect-{b}-delete-profile-button" for b in BackendKind)
_PROFILE_NAME_CONFIRM_BUTTON_IDS = tuple(f"connect-{b}-profile-name-confirm" for b in BackendKind)
_PROFILE_NAME_INPUT_IDS = tuple(f"connect-{b}-profile-name-input" for b in BackendKind)
_PROFILE_SELECT_IDS = tuple(f"connect-{b}-profile-select" for b in BackendKind)

#: Every widget group ``_set_fields_disabled`` toggles while a scan is in
#: flight -- disabling a container recursively disables every descendant.
#: Deliberately excludes ``#connect-actions``: the submit/cancel button
#: must stay clickable to cancel the scan.
_DISABLE_WHILE_SCANNING_IDS = (
    "connect-backend-tabs",
    "connect-local-fields",
    "connect-smb-fields",
    "connect-s3-fields",
    "connect-azure-fields",
)


#: What this dialog dismisses with on a successful scan: every repository
#: found, plus the scan label ``BrowseScreen`` shows as its path. ``None``
#: on Esc/cancel. Catalogs load lazily in ``BrowseScreen``.
ConnectResult = tuple[list[Repository], str]


class ConnectDialog(AppStateMixin, ModalScreen[ConnectResult | None]):
    """Keeps no ``COMMON_BINDINGS``: the App's ``q``/``d``/``?`` don't reach
    past a modal."""

    #: Which backend tab is active. ``init=False``: the initial value
    #: arrives via ``Tabs``'s first ``TabActivated``.
    backend: reactive[_Backend] = reactive(_Backend.LOCAL, init=False)

    #: Which tab's inline "save as profile" name row is open, if any --
    #: Esc while it's open closes only that row, not the whole dialog.
    #: ``init=False``: nothing is open at construction.
    naming_profile: reactive[BackendKind | None] = reactive(None, init=False)

    DEFAULT_CSS = (
        modal_box_css("ConnectDialog", width=84, guard_child_horizontal=True)
        + """
    #connect-backend-tabs {
        margin-bottom: 1;
    }

    #connect-local-fields, #connect-smb-fields, #connect-s3-fields, #connect-azure-fields {
        height: auto;
        display: none;
    }

    #connect-local-fields.active, #connect-smb-fields.active, #connect-s3-fields.active,
    #connect-azure-fields.active {
        display: block;
    }

    #connect-local-tree {
        height: 12;
        border: round $primary;
        margin-bottom: 1;
    }

    .pick-row {
        height: auto;
    }

    .pick-row Input, .profile-row Select, .profile-name-row Input {
        width: 1fr;
    }

    .pick-list {
        height: 6;
        border: round $primary;
        margin-bottom: 1;
        display: none;
    }

    .profile-row, .profile-name-row {
        height: auto;
        margin-bottom: 1;
    }

    .profile-name-row {
        display: none;
    }

    .pick-list.-visible, .profile-name-row.-visible {
        display: block;
    }

    #connect-actions {
        align: right middle;
    }
    """
    )

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("escape", "cancel", "Cancel", show=False),
        # Same jump-to-parent as NavigableScreen trees (this isn't one).
        # Leaving the rooted local directory goes via DirsOnlyTree's ".."
        # leaf. An Input claims backspace, so this never fires there.
        Binding("backspace", "cursor_to_parent", "To parent", show=False),
    ]

    def __init__(self) -> None:
        super().__init__()
        # The Worker running _scan(); None when no scan is in flight (see
        # ``scanning``). Not cleared on success, since the dialog dismisses.
        self._scan_worker: Worker[None] | None = None
        self._profiles = SavedProfileManager(self)
        self._remote_browser = RemoteOptionsBrowser(self)

    def _profile_rows(self, backend: BackendKind, select_prompt: str) -> ComposeResult:
        """A remote tab's saved-profile picker row and its hidden "save as" name row."""
        with Horizontal(id=f"connect-{backend}-profile-row", classes="profile-row"):
            yield Select[str]([], prompt=select_prompt, allow_blank=True, id=f"connect-{backend}-profile-select")
            yield Button(CONNECT_SAVE_PROFILE_LABEL, id=f"connect-{backend}-save-profile-button")
            yield Button(CONNECT_DELETE_PROFILE_LABEL, id=f"connect-{backend}-delete-profile-button", disabled=True)
        with Horizontal(id=f"connect-{backend}-profile-name-row", classes="profile-name-row"):
            yield Input(placeholder=CONNECT_PROFILE_NAME_PLACEHOLDER, id=f"connect-{backend}-profile-name-input")
            yield Button(CONNECT_PROFILE_NAME_CONFIRM_LABEL, id=f"connect-{backend}-profile-name-confirm")

    @override
    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(CONNECT_PROMPT)
            # One Tab stop, left/right switching the backend. Tab ids are the
            # _Backend values.
            yield Tabs(
                Tab(CONNECT_BACKEND_LOCAL_LABEL, id=_Backend.LOCAL),
                Tab(CONNECT_BACKEND_SMB_LABEL, id=_Backend.SMB),
                Tab(CONNECT_BACKEND_S3_LABEL, id=_Backend.S3),
                Tab(CONNECT_BACKEND_AZURE_LABEL, id=_Backend.AZURE),
                id="connect-backend-tabs",
            )
            with Vertical(id="connect-local-fields", classes="active"):
                yield Static(CONNECT_LOCAL_PROMPT)
                yield DirsOnlyTree(str(Path.cwd()), id="connect-local-tree")
                # select_on_focus=False: a keystroke edits the mostly-right
                # path rather than replacing it.
                yield Input(value=str(Path.cwd()), id="connect-local-path", select_on_focus=False)
            with Vertical(id="connect-smb-fields"):
                yield from self._profile_rows(BackendKind.SMB, CONNECT_SMB_PROFILE_SELECT_PROMPT)
                # No "Browse": SMB has no account-level share listing.
                yield Input(placeholder=CONNECT_SMB_SERVER_PLACEHOLDER, id="connect-smb-server")
                yield Input(placeholder=CONNECT_SMB_SHARE_PLACEHOLDER, id="connect-smb-share")
                yield Input(value=str(DEFAULT_SMB_PORT), id="connect-smb-port")
                yield Input(placeholder=CONNECT_SMB_USERNAME_PLACEHOLDER, id="connect-smb-username")
                yield Input(placeholder=CONNECT_SMB_PASSWORD_PLACEHOLDER, password=True, id="connect-smb-password")
            with Vertical(id="connect-s3-fields"):
                yield from self._profile_rows(BackendKind.S3, CONNECT_S3_PROFILE_SELECT_PROMPT)
                # Bucket last: "Browse" builds its client from the fields
                # above it.
                yield Input(placeholder=CONNECT_S3_ACCESS_KEY_PLACEHOLDER, id="connect-s3-access-key")
                yield Input(placeholder=CONNECT_S3_SECRET_KEY_PLACEHOLDER, password=True, id="connect-s3-secret-key")
                yield Input(placeholder=CONNECT_S3_ENDPOINT_PLACEHOLDER, id="connect-s3-endpoint")
                yield Input(placeholder=CONNECT_S3_REGION_PLACEHOLDER, id="connect-s3-region")
                # No prefix field: a repository always sits at the bucket
                # root (likewise for an Azure container).
                with Horizontal(id="connect-s3-bucket-row", classes="pick-row"):
                    yield Input(placeholder=CONNECT_S3_BUCKET_PLACEHOLDER, id="connect-s3-bucket")
                    yield Button(CONNECT_S3_BROWSE_BUCKETS_LABEL, id="connect-s3-browse-buckets")
                yield OptionList(id="connect-s3-bucket-list", classes="pick-list")
                yield Checkbox(CONNECT_VERIFY_TLS_LABEL, value=True, id="connect-s3-verify-tls")
            with Vertical(id="connect-azure-fields"):
                yield from self._profile_rows(BackendKind.AZURE, CONNECT_AZURE_PROFILE_SELECT_PROMPT)
                # Container last, as on the S3 tab. AzureStore takes an
                # account URL and credential, not a connection string.
                yield Input(placeholder=CONNECT_AZURE_ACCOUNT_URL_PLACEHOLDER, id="connect-azure-account-url")
                yield Input(
                    placeholder=CONNECT_AZURE_CREDENTIAL_PLACEHOLDER, password=True, id="connect-azure-credential"
                )
                with Horizontal(id="connect-azure-container-row", classes="pick-row"):
                    yield Input(placeholder=CONNECT_AZURE_CONTAINER_PLACEHOLDER, id="connect-azure-container")
                    yield Button(CONNECT_AZURE_BROWSE_CONTAINERS_LABEL, id="connect-azure-browse-containers")
                yield OptionList(id="connect-azure-container-list", classes="pick-list")
                yield Checkbox(CONNECT_VERIFY_TLS_LABEL, value=True, id="connect-azure-verify-tls")
            with Horizontal(id="connect-actions"):
                yield Button(CONNECT_SUBMIT_LABEL_LOCAL, id="connect-submit", variant="primary")
            yield Static("", id="connect-status")

    def on_mount(self) -> None:
        self.query_one("#connect-local-tree", DirsOnlyTree).focus()
        self.refresh_profile_lists()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id == "connect-submit":
            # Submit or cancel, depending on whether a scan is in flight.
            self._cancel_scan() if self.scanning else self._submit()
        elif button_id == "connect-s3-browse-buckets":
            self._browse_buckets()
        elif button_id == "connect-azure-browse-containers":
            self._browse_containers()
        elif button_id in _SAVE_PROFILE_BUTTON_IDS:
            self._show_profile_name_row(profile_backend_of(button_id))
        elif button_id in _DELETE_PROFILE_BUTTON_IDS:
            self._delete_selected_profile(profile_backend_of(button_id))
        elif button_id in _PROFILE_NAME_CONFIRM_BUTTON_IDS:
            self._confirm_save_profile(profile_backend_of(button_id))

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        self.backend = cast(_Backend, event.tab.id)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        input_id = event.input.id
        if input_id in _PROFILE_NAME_INPUT_IDS:
            self._confirm_save_profile(profile_backend_of(input_id))
            return
        # Enter, from any other field, confirms — same as clicking the submit
        # button (ExportScreen's dst Input has the same convention).
        self._submit()

    def on_select_changed(self, event: Select.Changed) -> None:
        select_id = event.select.id
        if select_id not in _PROFILE_SELECT_IDS:
            return
        backend = profile_backend_of(select_id)
        self.query_one(f"#connect-{backend}-delete-profile-button", Button).disabled = not isinstance(event.value, str)
        if isinstance(event.value, str):
            self._load_selected_profile(backend, event.value)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        option_list = event.option_list
        if option_list.id == "connect-s3-bucket-list":
            target = self.query_one("#connect-s3-bucket", Input)
        elif option_list.id == "connect-azure-container-list":
            target = self.query_one("#connect-azure-container", Input)
        else:
            return
        target.value = str(event.option.prompt)
        option_list.remove_class("-visible")
        target.focus()

    def on_directory_tree_directory_selected(self, event: DirectoryTree.DirectorySelected) -> None:
        # Keeps the typable Input in sync with whatever the tree selected
        # (DirEntry.path already resolves ".." to the real parent), so
        # either interaction path leaves the same field
        # _build_local_store() reads from.
        self.query_one("#connect-local-path", Input).value = str(event.path)
        if str(event.node.label) == "..":
            # Re-root the tree at the parent instead of leaving ".." inert.
            self.query_one("#connect-local-tree", DirsOnlyTree).path = str(event.path)

    def action_cursor_to_parent(self) -> None:
        if isinstance(self.focused, Tree):
            move_cursor_to_parent(self.focused)

    def watch_naming_profile(self, old: BackendKind | None, new: BackendKind | None) -> None:
        if old is not None:
            self.query_one(f"#connect-{old}-profile-name-row").remove_class("-visible")
        if new is not None:
            name_input = self.query_one(f"#connect-{new}-profile-name-input", Input)
            name_input.value = ""
            self.query_one(f"#connect-{new}-profile-name-row").add_class("-visible")
            name_input.focus()

    def on_saved_profile_manager_profiles_refreshed(self, message: SavedProfileManager.ProfilesRefreshed) -> None:
        for backend in BackendKind:
            names = [p.name for p in message.profiles if p.kind is backend]
            self.query_one(f"#connect-{backend}-profile-select", Select).set_options([(name, name) for name in names])

    def on_remote_options_browser_items_listed(self, message: RemoteOptionsBrowser.ItemsListed) -> None:
        if message.error is not None:
            show_error(self, "#connect-status", message.error)
            return
        option_list = self.query_one(message.option_list_id, OptionList)
        if not message.items:
            self.query_one("#connect-status", Static).update(f"no {message.noun}s found")
            return
        option_list.clear_options()
        for item in message.items:
            option_list.add_option(item)
        # OptionList.highlighted starts None -- Enter's action_select is a
        # no-op with nothing highlighted, so the first option is
        # highlighted explicitly.
        option_list.highlighted = 0
        option_list.add_class("-visible")
        option_list.focus()
        found = len(message.items)
        self.query_one("#connect-status", Static).update(f"found {found} {pluralize(found, message.noun)} — pick one")

    def _watch_backend(self, backend: _Backend) -> None:
        # Leaves focus alone: moving it here would end arrow-key browsing
        # of the tabs strip after one press.
        self._profiles.hide_name_row()
        for key in _Backend:
            self.query_one(f"#connect-{key}-fields").set_class(key == backend, "active")
        # Never reached mid-scan -- the tabs strip is disabled while a
        # scan is in flight.
        self.query_one("#connect-submit", Button).label = self._submit_label()

    def _submit_label(self) -> str:
        return CONNECT_SUBMIT_LABEL_LOCAL if self.backend == _Backend.LOCAL else CONNECT_SUBMIT_LABEL

    def _set_fields_disabled(self, disabled: bool) -> None:
        """Toggles ``disabled`` on every ``_DISABLE_WHILE_SCANNING_IDS`` group."""
        for widget_id in _DISABLE_WHILE_SCANNING_IDS:
            self.query_one(f"#{widget_id}").disabled = disabled

    def _end_scan(self) -> None:
        """Restores the editable state once a scan fails or is cancelled.
        Tolerates the screen already being gone: cancellation cleanup can
        run after ``action_cancel`` dismissed it."""
        self._scan_worker = None
        with contextlib.suppress(NoMatches):
            self._set_fields_disabled(False)
            self.query_one("#connect-submit", Button).label = self._submit_label()
            # Clear scan-in-progress text (_fail_scan then overwrites it).
            self.query_one("#connect-status", Static).update("")

    def _cancel_scan(self) -> None:
        if self._scan_worker is not None:
            self._scan_worker.cancel()
            # Immediate feedback: Worker.cancel() only requests cancellation,
            # so _end_scan's clear runs later, when CancelledError lands.
            self.query_one("#connect-status", Static).update(CONNECT_CANCELLING_STATUS)

    def action_cancel(self) -> None:
        if self.naming_profile is not None:
            self._profiles.hide_name_row()
            return
        # Also stop a scan in flight, or it would keep running orphaned.
        self._cancel_scan()
        self.dismiss(None)

    # -- store construction/validation -----------------------------------
    # A worker because _build_store awaits remote_store. busy=False:
    # store construction does no network I/O, and _scan reports progress.
    @work(busy=False)
    async def _submit(self) -> None:
        if self.scanning or any(self._remote_browser.browsing.values()):
            return
        result = await self._validated_store(self._build_store)
        if result is None:
            return
        store, label = result
        self._set_fields_disabled(True)
        # Now the Cancel button; stays enabled.
        self.query_one("#connect-submit", Button).label = CONNECT_CANCEL_LABEL
        self._scan_worker = self._scan(store, label)

    async def _validated_store(
        self, build: Callable[[], Awaitable[tuple[ObjectStore, str]]]
    ) -> tuple[ObjectStore, str] | None:
        """Runs ``build``, showing any failure via ``show_error`` and
        returning ``None``. Shared by ``_submit`` and ``validated_store_for``."""
        try:
            return await build()
        except Exception as exc:  # noqa: BLE001
            # A field problem: ``ConnectValidationError``, or Azure rejecting
            # its account URL (``ProfileFieldError``).
            show_error(self, "#connect-status", exc)
            return None

    async def validated_store_for(self, backend: BackendKind) -> tuple[ObjectStore, str] | None:
        """``_submit()``'s validation for one backend, used by
        ``SavedProfileManager.show_name_row`` before the "save as profile"
        prompt; no live connection is required."""
        return await self._validated_store(lambda: self._build_remote_store(backend))

    @property
    def scanning(self) -> bool:
        """Whether a ``_scan()`` is in flight; guards ``_submit`` and
        ``RemoteOptionsBrowser.browse()``."""
        return self._scan_worker is not None

    async def _build_store(self) -> tuple[ObjectStore, str]:
        if self.backend == _Backend.LOCAL:
            return self._build_local_store()
        return await self._build_remote_store(BackendKind(self.backend))

    def _build_local_store(self) -> tuple[ObjectStore, str]:
        return local_store(self.query_one("#connect-local-path", Input).value)

    async def _build_remote_store(self, backend: BackendKind) -> tuple[ObjectStore, str]:
        return await remote_store(backend, self._profiles.fields(backend))

    def account_client_kwargs(self, backend: BackendKind) -> dict[str, object]:
        """The bucket-/container-less client kwargs ``RemoteOptionsBrowser``
        lists buckets/containers with, from the tab's current fields."""
        return account_client_kwargs(backend, self._profiles.fields(backend))

    # -- saved profiles (SavedProfileManager) -----------------------------
    # Thin @work wrappers (Textual's @work binds to the defining Screen);
    # bodies live on self._profiles. busy=False: local disk I/O only.

    @work(busy=False)
    async def _show_profile_name_row(self, backend: BackendKind) -> None:
        await self._profiles.show_name_row(backend)

    # Runs on mount and after every save/delete.
    @work(busy=False)
    async def refresh_profile_lists(self) -> None:
        await self._profiles.refresh_lists()

    @work(busy=False)
    async def _load_selected_profile(self, backend: BackendKind, name: str) -> None:
        await self._profiles.load_selected(backend, name)

    @work(busy=False)
    async def _confirm_save_profile(self, backend: BackendKind) -> None:
        await self._profiles.confirm_save(backend)

    # Deletes immediately, with no confirmation (as WorklistScreen's ``x``).
    @work(busy=False)
    async def _delete_selected_profile(self, backend: BackendKind) -> None:
        await self._profiles.delete_selected(backend)

    # -- remote bucket/container browsing (RemoteOptionsBrowser) ----------
    # Thin wrappers; the busy indicator lives in RemoteOptionsBrowser.

    @work(busy=False)
    async def _browse_buckets(self) -> None:
        await self._remote_browser.browse_buckets()

    @work(busy=False)
    async def _browse_containers(self) -> None:
        await self._remote_browser.browse_containers()

    # -- scan --------------------------------------------------------------
    # One scan path for every backend, local included. busy=False: it
    # writes its own progress to #connect-status.
    @work(busy=False)
    async def _scan(self, store: ObjectStore, label: str) -> None:
        status = self.query_one("#connect-status", Static)
        # repr() before safe(), so repr() doesn't escape safe()'s marks.
        status.update(f"scanning {safe(repr(label))}...")
        session = self.app_state.session

        async def on_progress(p: Progress) -> None:
            found = p.found if p.found is not None else 0
            repos = pluralize(found, "repository", "repositories")
            status.update(f"scanning... found {found} {repos}")

        try:
            repos = await scan_repositories(session, store, label, on_progress=on_progress)
        except asyncio.CancelledError:
            # Re-raise so the Worker ends CANCELLED, not FAILED. On Esc this
            # runs after dismiss(None); _end_scan tolerates that.
            self._end_scan()
            raise
        except Exception as exc:  # noqa: BLE001
            # A storage or format failure: expected, not a bug.
            self._fail_scan(exc)
            return
        self.dismiss((repos, label))

    def _fail_scan(self, message: object) -> None:
        self._end_scan()
        show_error(self, "#connect-status", message)
