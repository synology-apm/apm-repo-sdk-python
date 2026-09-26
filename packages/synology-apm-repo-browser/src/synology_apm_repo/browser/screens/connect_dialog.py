"""``ConnectDialog`` is a small, centered modal overlay -- same treatment
as ``KeyDialog``/``ExportScreen`` -- for picking *where* to browse: a
local directory, an S3/Azure Blob Storage-backed repository, or an SMB
share. Auto-opened once on app start; ``c`` reopens it later from
``BrowseScreen``. This is the only place a source is ever picked.

The connection test/repository scan runs here, not in ``BrowseScreen``:
``_scan`` drives ``Session.discover_remote`` for every backend, streaming
"found N so far" progress into ``#connect-status``, and dismisses only
once at least one repository was found -- a validation error,
connectivity failure, or empty scan all surface inline here.

Local browsing is ``DirsOnlyTree`` -- filters to directories only, and
adds a synthetic ``".."`` entry plus type-ahead navigation, paired with a
plain path ``Input`` that mirrors the tree's current selection.

Two collaborators, held privately, reaching back into this dialog only
through the surface exposed for that (same convention as
``goto_walker.py``'s ``GotoChainWalker``): ``profile_manager.
SavedProfileManager`` (saved-connection-profile CRUD) and
``remote_browser.RemoteOptionsBrowser`` (the bucket/container "Browse"
flow). This dialog keeps backend selection, store construction/
validation, and the scan itself.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import cast

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.css.query import NoMatches
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, DirectoryTree, Input, OptionList, Select, Static, Tab, Tabs, Tree
from textual.worker import Worker

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.core.connect.validate import (
    ConnectValidationError,
    azure_config_and_secrets,
    s3_config_and_secrets,
    smb_config_and_secrets,
    validate_azure,
    validate_local,
    validate_s3,
)
from synology_apm_repo.browser.screens._shared import modal_box_css, move_cursor_to_parent, show_error
from synology_apm_repo.browser.screens.profile_manager import (
    _BACKEND_KIND,
    SavedProfileManager,
    _ProfileBackend,
    profile_backend_of,
)
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
    CONNECT_S3_VERIFY_TLS_LABEL,
    CONNECT_SAVE_PROFILE_LABEL,
    CONNECT_SMB_PASSWORD_PLACEHOLDER,
    CONNECT_SMB_PROFILE_SELECT_PROMPT,
    CONNECT_SMB_SERVER_PLACEHOLDER,
    CONNECT_SMB_SHARE_PLACEHOLDER,
    CONNECT_SMB_USERNAME_PLACEHOLDER,
    CONNECT_SUBMIT_LABEL,
    CONNECT_SUBMIT_LABEL_LOCAL,
)
from synology_apm_repo.browser.widgets.dirs_only_tree import DirsOnlyTree
from synology_apm_repo.browser.widgets.worker_progress import work
from synology_apm_repo.sdk.api import Repository
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.presentation.format import pluralize
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.presentation.progress import Progress, ProgressMeter
from synology_apm_repo.sdk.profiles import (
    AzureProfileConfig,
    BackendKind,
    S3ProfileConfig,
    client_kwargs_with_secrets,
    store_from_config,
)
from synology_apm_repo.sdk.storage import LocalFsStore, ObjectStore


class _Backend(enum.StrEnum):
    LOCAL = "local"
    SMB = "smb"
    S3 = "s3"
    AZURE = "azure"


#: Every profile-related widget id, generated from ``_ProfileBackend``
#: rather than spelled out per backend — a fourth backend then needs only
#: the enum touched, not these five tuples too.
_SAVE_PROFILE_BUTTON_IDS = tuple(f"connect-{b}-save-profile-button" for b in _ProfileBackend)
_DELETE_PROFILE_BUTTON_IDS = tuple(f"connect-{b}-delete-profile-button" for b in _ProfileBackend)
_PROFILE_NAME_CONFIRM_BUTTON_IDS = tuple(f"connect-{b}-profile-name-confirm" for b in _ProfileBackend)
_PROFILE_NAME_INPUT_IDS = tuple(f"connect-{b}-profile-name-input" for b in _ProfileBackend)
_PROFILE_SELECT_IDS = tuple(f"connect-{b}-profile-select" for b in _ProfileBackend)

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
#: found, plus the same display label ``BrowseScreen._repo_label`` needs.
#: ``None`` on Esc/cancel. Connections aren't fetched here --
#: ``BrowseScreen`` loads each repository's connections lazily, since
#: fetching from S3/Azure is a real network cost.
ConnectResult = tuple[list[Repository], str]


class ConnectDialog(ModalScreen[ConnectResult | None]):
    """``ModalScreen`` truncates the App-level binding chain at itself,
    same as ``KeyDialog``/``ExportScreen``."""

    #: Which backend tab is active. ``init=False``: the real initial value
    #: comes from ``Tabs``'s own first ``TabActivated``, not this reactive
    #: firing its watcher twice.
    backend: reactive[_Backend] = reactive(_Backend.LOCAL, init=False)

    #: Which tab's inline "save as profile" name row is open, if any --
    #: Esc while it's open closes only that row, not the whole dialog.
    #: ``init=False``: nothing is open at construction.
    naming_profile: reactive[_ProfileBackend | None] = reactive(None, init=False)

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

    #connect-s3-bucket-row, #connect-azure-container-row {
        height: auto;
    }

    #connect-s3-bucket-row Input, #connect-azure-container-row Input {
        width: 1fr;
    }

    #connect-s3-bucket-list, #connect-azure-container-list {
        height: 6;
        border: round $primary;
        margin-bottom: 1;
        display: none;
    }

    #connect-s3-bucket-list.-visible, #connect-azure-container-list.-visible {
        display: block;
    }

    #connect-smb-profile-row, #connect-s3-profile-row, #connect-azure-profile-row {
        height: auto;
        margin-bottom: 1;
    }

    #connect-smb-profile-row Select, #connect-s3-profile-row Select, #connect-azure-profile-row Select {
        width: 1fr;
    }

    #connect-smb-profile-name-row, #connect-s3-profile-name-row, #connect-azure-profile-name-row {
        height: auto;
        margin-bottom: 1;
        display: none;
    }

    #connect-smb-profile-name-row.-visible, #connect-s3-profile-name-row.-visible,
    #connect-azure-profile-name-row.-visible {
        display: block;
    }

    #connect-smb-profile-name-row Input, #connect-s3-profile-name-row Input,
    #connect-azure-profile-name-row Input {
        width: 1fr;
    }

    #connect-actions {
        align: right middle;
    }
    """
    )

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=False),
        # Same shared "jump to parent, collapse it" behavior every other
        # tree gets via NavigableScreen -- reached directly since
        # ConnectDialog isn't one. Going up out of the rooted local
        # directory is different: DirsOnlyTree handles that via its own
        # synthetic ".." leaf. Never reached while an Input has focus --
        # Input already claims backspace.
        Binding("backspace", "cursor_to_parent", "To parent", show=False),
    ]

    def __init__(self) -> None:
        super().__init__()
        # The real Worker _scan() is running as -- None when no scan is in
        # flight, doubling as the ``scanning`` property's flag: a guard
        # against a second submit mid-scan, and against
        # ``RemoteOptionsBrowser.browse()`` listing while a scan runs.
        # Textual's @work returns this synchronously, so there's no race
        # between _submit() assigning it and a user pressing Cancel.
        # Never cleared on a successful scan since the dialog dismisses.
        self._scan_worker: Worker[None] | None = None
        self._profiles = SavedProfileManager(self)
        self._remote_browser = RemoteOptionsBrowser(self)

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(CONNECT_PROMPT)
            # A single Tabs strip, not three Buttons: one Tab-stop for this
            # whole choice, left/right switching the active backend. Tab
            # ids double as the _Backend literal values, so there's no
            # separate id->backend mapping.
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
                # select_on_focus=False: Input's default (True) turns the
                # first keystroke into "replace everything" instead of
                # "insert at cursor" -- wrong for a field that already
                # holds a mostly-right path.
                yield Input(value=str(Path.cwd()), id="connect-local-path", select_on_focus=False)
            with Vertical(id="connect-smb-fields"):
                with Horizontal(id="connect-smb-profile-row"):
                    yield Select[str](
                        [],
                        prompt=CONNECT_SMB_PROFILE_SELECT_PROMPT,
                        allow_blank=True,
                        id="connect-smb-profile-select",
                    )
                    yield Button(CONNECT_SAVE_PROFILE_LABEL, id="connect-smb-save-profile-button")
                    yield Button(CONNECT_DELETE_PROFILE_LABEL, id="connect-smb-delete-profile-button", disabled=True)
                with Horizontal(id="connect-smb-profile-name-row"):
                    yield Input(placeholder=CONNECT_PROFILE_NAME_PLACEHOLDER, id="connect-smb-profile-name-input")
                    yield Button(CONNECT_PROFILE_NAME_CONFIRM_LABEL, id="connect-smb-profile-name-confirm")
                # No "Browse" button here, unlike the S3/Azure tabs below:
                # an SMB server has no single account-level "list every
                # share" operation the way S3/Azure list buckets/containers,
                # so the share name is always typed directly.
                yield Input(placeholder=CONNECT_SMB_SERVER_PLACEHOLDER, id="connect-smb-server")
                yield Input(placeholder=CONNECT_SMB_SHARE_PLACEHOLDER, id="connect-smb-share")
                yield Input(value="445", id="connect-smb-port")
                yield Input(placeholder=CONNECT_SMB_USERNAME_PLACEHOLDER, id="connect-smb-username")
                yield Input(placeholder=CONNECT_SMB_PASSWORD_PLACEHOLDER, password=True, id="connect-smb-password")
            with Vertical(id="connect-s3-fields"):
                with Horizontal(id="connect-s3-profile-row"):
                    yield Select[str](
                        [],
                        prompt=CONNECT_S3_PROFILE_SELECT_PROMPT,
                        allow_blank=True,
                        id="connect-s3-profile-select",
                    )
                    yield Button(CONNECT_SAVE_PROFILE_LABEL, id="connect-s3-save-profile-button")
                    yield Button(CONNECT_DELETE_PROFILE_LABEL, id="connect-s3-delete-profile-button", disabled=True)
                with Horizontal(id="connect-s3-profile-name-row"):
                    yield Input(placeholder=CONNECT_PROFILE_NAME_PLACEHOLDER, id="connect-s3-profile-name-input")
                    yield Button(CONNECT_PROFILE_NAME_CONFIRM_LABEL, id="connect-s3-profile-name-confirm")
                # Credentials/endpoint/region first, bucket last: the
                # "Browse" button builds its client from whatever's
                # already filled in above it, so the fields it depends on
                # come before the field it populates.
                yield Input(placeholder=CONNECT_S3_ACCESS_KEY_PLACEHOLDER, id="connect-s3-access-key")
                yield Input(placeholder=CONNECT_S3_SECRET_KEY_PLACEHOLDER, password=True, id="connect-s3-secret-key")
                yield Input(placeholder=CONNECT_S3_ENDPOINT_PLACEHOLDER, id="connect-s3-endpoint")
                yield Input(placeholder=CONNECT_S3_REGION_PLACEHOLDER, id="connect-s3-region")
                # No "prefix" field: ActiveProtect's own provisioning always
                # writes its marker directory as the bucket's first path
                # segment -- there's no real repository to reach scoping
                # narrower than the bucket root (same for Azure below).
                with Horizontal(id="connect-s3-bucket-row"):
                    yield Input(placeholder=CONNECT_S3_BUCKET_PLACEHOLDER, id="connect-s3-bucket")
                    yield Button(CONNECT_S3_BROWSE_BUCKETS_LABEL, id="connect-s3-browse-buckets")
                yield OptionList(id="connect-s3-bucket-list")
                yield Checkbox(CONNECT_S3_VERIFY_TLS_LABEL, value=True, id="connect-s3-verify-tls")
            with Vertical(id="connect-azure-fields"):
                with Horizontal(id="connect-azure-profile-row"):
                    yield Select[str](
                        [],
                        prompt=CONNECT_AZURE_PROFILE_SELECT_PROMPT,
                        allow_blank=True,
                        id="connect-azure-profile-select",
                    )
                    yield Button(CONNECT_SAVE_PROFILE_LABEL, id="connect-azure-save-profile-button")
                    yield Button(CONNECT_DELETE_PROFILE_LABEL, id="connect-azure-delete-profile-button", disabled=True)
                with Horizontal(id="connect-azure-profile-name-row"):
                    yield Input(placeholder=CONNECT_PROFILE_NAME_PLACEHOLDER, id="connect-azure-profile-name-input")
                    yield Button(CONNECT_PROFILE_NAME_CONFIRM_LABEL, id="connect-azure-profile-name-confirm")
                # Account URL/credential first, container last -- same
                # ordering as the S3 tab's credentials-before-bucket layout.
                #
                # No "connection string" field: AzureStore builds its client
                # via BlobServiceClient(**client_kwargs), not
                # from_connection_string(), so only account_url/credential
                # are offered here.
                yield Input(placeholder=CONNECT_AZURE_ACCOUNT_URL_PLACEHOLDER, id="connect-azure-account-url")
                yield Input(
                    placeholder=CONNECT_AZURE_CREDENTIAL_PLACEHOLDER, password=True, id="connect-azure-credential"
                )
                with Horizontal(id="connect-azure-container-row"):
                    yield Input(placeholder=CONNECT_AZURE_CONTAINER_PLACEHOLDER, id="connect-azure-container")
                    yield Button(CONNECT_AZURE_BROWSE_CONTAINERS_LABEL, id="connect-azure-browse-containers")
                yield OptionList(id="connect-azure-container-list")
            with Horizontal(id="connect-actions"):
                yield Button(CONNECT_SUBMIT_LABEL_LOCAL, id="connect-submit", variant="primary")
            yield Static("", id="connect-status")

    def on_mount(self) -> None:
        self.query_one("#connect-local-tree", DirsOnlyTree).focus()
        self.refresh_profile_lists()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id == "connect-submit":
            # This one button is submit or cancel depending on whether a
            # scan is currently in flight -- its own label already says
            # which.
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
            # The one thing needing special-casing: re-root the whole
            # tree at the parent instead of leaving ".." as an inert leaf.
            # No real directory can be named exactly "..".
            self.query_one("#connect-local-tree", DirsOnlyTree).path = str(event.path)

    def action_cursor_to_parent(self) -> None:
        if isinstance(self.focused, Tree):
            move_cursor_to_parent(self.focused)

    def watch_naming_profile(self, old: _ProfileBackend | None, new: _ProfileBackend | None) -> None:
        if old is not None:
            self.query_one(f"#connect-{old}-profile-name-row").remove_class("-visible")
        if new is not None:
            name_input = self.query_one(f"#connect-{new}-profile-name-input", Input)
            name_input.value = ""
            self.query_one(f"#connect-{new}-profile-name-row").add_class("-visible")
            name_input.focus()

    def on_saved_profile_manager_profiles_refreshed(self, message: SavedProfileManager.ProfilesRefreshed) -> None:
        for backend, kind in _BACKEND_KIND.items():
            names = [p.name for p in message.profiles if p.kind is kind]
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
        # Deliberately leaves focus wherever it already is rather than
        # moving it into the newly-active group: this runs on every
        # Tabs.TabActivated including left/right while the tabs strip has
        # focus, and moving focus here would end arrow-key backend
        # browsing after one press. Tab moves focus into the active group.
        self._profiles.hide_name_row()
        for key in _Backend:
            self.query_one(f"#connect-{key}-fields").set_class(key == backend, "active")
        # Never reached mid-scan -- the tabs strip is disabled while a
        # scan is in flight.
        self.query_one("#connect-submit", Button).label = self._submit_label()

    def _submit_label(self) -> str:
        return CONNECT_SUBMIT_LABEL_LOCAL if self.backend == _Backend.LOCAL else CONNECT_SUBMIT_LABEL

    def _set_fields_disabled(self, disabled: bool) -> None:
        """Toggles every backend-picker/field-group widget's ``disabled``
        while a scan is in flight, so nothing here can be edited out from
        under it. ``#connect-actions`` is deliberately excluded -- it
        must stay clickable to cancel the scan."""
        for widget_id in _DISABLE_WHILE_SCANNING_IDS:
            self.query_one(f"#{widget_id}").disabled = disabled

    def _end_scan(self) -> None:
        """Restores this dialog to its normal, editable state once a scan
        stops (failure or cancellation -- a successful scan dismisses the
        whole screen instead). Shared by ``_fail_scan``/``_cancel_scan``
        so neither leaves a remnant of the other. Tolerates the screen
        already being gone: a cancellation's cleanup can run after
        ``action_cancel`` already dismissed the screen, since
        ``Worker.cancel()`` only requests cancellation."""
        self._scan_worker = None
        with contextlib.suppress(NoMatches):
            self._set_fields_disabled(False)
            self.query_one("#connect-submit", Button).label = self._submit_label()
            # Clears whatever scan-in-progress text is still showing --
            # _fail_scan's show_error() immediately overwrites this with
            # the real error, so a failure never flickers through blank status.
            self.query_one("#connect-status", Static).update("")

    def _cancel_scan(self) -> None:
        if self._scan_worker is not None:
            self._scan_worker.cancel()
            # Immediate feedback -- _end_scan's own clear (above) doesn't
            # run until the real CancelledError actually lands in _scan
            # (Worker.cancel() only requests cancellation), an
            # async-boundary-away moment after this click.
            self.query_one("#connect-status", Static).update(CONNECT_CANCELLING_STATUS)

    def action_cancel(self) -> None:
        # Esc while the inline "save as profile" name row is open closes
        # only that row, not the whole dialog.
        if self.naming_profile is not None:
            self._profiles.hide_name_row()
            return
        # Stop a scan in flight too, not just close the dialog on top of
        # it -- otherwise it keeps running orphaned in the background.
        self._cancel_scan()
        self.dismiss(None)

    # -- store construction/validation -----------------------------------
    # _build_store awaits store_from_config for s3/azure, so this and
    # validated_store_for's own callers must run as workers.
    # busy=False: constructing a store never performs network I/O, so
    # there's nothing here worth a busy indicator -- the real scan
    # (_scan, below) already reports its own progress.
    @work(busy=False)
    async def _submit(self) -> None:
        if self.scanning or any(self._remote_browser.browsing.values()):
            return
        result = await self._validated_store(self._build_store)
        if result is None:
            return
        store, label = result
        self._set_fields_disabled(True)
        # Stays enabled: it's now the Cancel button, the only widget in
        # this dialog still meant to be clickable while a scan is running.
        self.query_one("#connect-submit", Button).label = CONNECT_CANCEL_LABEL
        self._scan_worker = self._scan(store, label)

    async def _validated_store(
        self, build: Callable[[], Awaitable[tuple[ObjectStore, str]]]
    ) -> tuple[ObjectStore, str] | None:
        """Runs ``build``, translating a validation failure into
        ``show_error`` and returning ``None`` -- shared by ``_submit``
        (uses the built store) and ``validated_store_for`` (discards
        it)."""
        try:
            return await build()
        except ConnectValidationError as exc:
            # A field this dialog can check before ever scanning (empty
            # path/bucket/container/server/share, a non-directory local
            # path, a non-numeric SMB port) -- no I/O involved.
            show_error(self, "#connect-status", exc)
            return None
        except Exception as exc:  # Constructing a store can still raise synchronously
            # (a malformed Azure account URL, an unresolvable account name)
            # even though it performs no network I/O -- an expected,
            # common construction-time failure, not a bug to crash over.
            show_error(self, "#connect-status", exc)
            return None

    async def validated_store_for(self, backend: _ProfileBackend) -> tuple[ObjectStore, str] | None:
        """Same validation ``_submit()`` runs before scanning, for one
        named backend -- ``SavedProfileManager.show_name_row`` uses this
        to catch an obviously-missing field before showing the "save as
        profile" prompt; a live connection is never required for that."""
        if backend == _ProfileBackend.S3:
            build = self._build_s3_store
        elif backend == _ProfileBackend.AZURE:
            build = self._build_azure_store
        else:
            build = self._build_smb_store
        return await self._validated_store(build)

    @property
    def scanning(self) -> bool:
        """Whether ``_scan()`` currently has a connectivity check in
        flight -- ``RemoteOptionsBrowser.browse()``'s guard against
        browsing mid-scan, and ``_submit``'s guard against a second
        submit."""
        return self._scan_worker is not None

    async def _build_store(self) -> tuple[ObjectStore, str]:
        if self.backend == _Backend.LOCAL:
            return self._build_local_store()
        if self.backend == _Backend.S3:
            return await self._build_s3_store()
        if self.backend == _Backend.AZURE:
            return await self._build_azure_store()
        return await self._build_smb_store()

    def _build_local_store(self) -> tuple[ObjectStore, str]:
        raw = self.query_one("#connect-local-path", Input).value.strip()
        path = validate_local(raw)
        return LocalFsStore(path), str(path)

    def _s3_config_and_secrets(self, bucket: str) -> tuple[S3ProfileConfig, dict[str, str]]:
        """Reads every S3 tab field and delegates config/secret-source
        construction to ``core/connect/validate.py``'s pure
        ``s3_config_and_secrets``. ``bucket`` decides whether the caller
        wants the bucket-less placeholder or a chosen bucket -- neither
        validates ``bucket`` here (``validate_s3`` does)."""
        return s3_config_and_secrets(
            bucket=bucket,
            endpoint=self.query_one("#connect-s3-endpoint", Input).value.strip(),
            region=self.query_one("#connect-s3-region", Input).value.strip(),
            access_key=self.query_one("#connect-s3-access-key", Input).value.strip(),
            secret_key=self.query_one("#connect-s3-secret-key", Input).value,
            verify_tls=self.query_one("#connect-s3-verify-tls", Checkbox).value,
        )

    def s3_client_kwargs(self) -> dict[str, object]:
        """The bucket-less resolved client kwargs
        ``RemoteOptionsBrowser.browse_buckets`` needs, built via
        ``client_kwargs_with_secrets`` directly."""
        config, secret_source = self._s3_config_and_secrets("")
        return client_kwargs_with_secrets(config, secret_source)

    async def _build_s3_store(self) -> tuple[ObjectStore, str]:
        bucket = self.query_one("#connect-s3-bucket", Input).value.strip()
        # validate_s3 re-derives config/secret_source from the same
        # fields _s3_config_and_secrets reads -- keeps that pure split
        # the single source of truth for "valid", rather than
        # re-deriving it here.
        config, secret_source = validate_s3(
            bucket=bucket,
            endpoint=self.query_one("#connect-s3-endpoint", Input).value.strip(),
            region=self.query_one("#connect-s3-region", Input).value.strip(),
            access_key=self.query_one("#connect-s3-access-key", Input).value.strip(),
            secret_key=self.query_one("#connect-s3-secret-key", Input).value,
            verify_tls=self.query_one("#connect-s3-verify-tls", Checkbox).value,
        )
        store = await store_from_config(BackendKind.S3, config, secret_source)
        return store, f"s3://{bucket}"

    def _azure_config_and_secrets(self, container: str) -> tuple[AzureProfileConfig, dict[str, str]]:
        """Same role as ``_s3_config_and_secrets`` -- delegates to
        ``core/connect/validate.py``'s pure ``azure_config_and_secrets``."""
        return azure_config_and_secrets(
            container=container,
            account_url=self.query_one("#connect-azure-account-url", Input).value.strip(),
            credential=self.query_one("#connect-azure-credential", Input).value,
        )

    def azure_client_kwargs(self) -> dict[str, object]:
        """The container-less resolved client kwargs
        ``RemoteOptionsBrowser.browse_containers`` needs, built via
        ``client_kwargs_with_secrets`` directly."""
        config, secret_source = self._azure_config_and_secrets("")
        return client_kwargs_with_secrets(config, secret_source)

    async def _build_azure_store(self) -> tuple[ObjectStore, str]:
        container = self.query_one("#connect-azure-container", Input).value.strip()
        config, secret_source = validate_azure(
            container=container,
            account_url=self.query_one("#connect-azure-account-url", Input).value.strip(),
            credential=self.query_one("#connect-azure-credential", Input).value,
        )
        store = await store_from_config(BackendKind.AZURE, config, secret_source)
        return store, f"azure://{container}"

    async def _build_smb_store(self) -> tuple[ObjectStore, str]:
        server = self.query_one("#connect-smb-server", Input).value.strip()
        share = self.query_one("#connect-smb-share", Input).value.strip()
        port_text = self.query_one("#connect-smb-port", Input).value.strip()
        username = self.query_one("#connect-smb-username", Input).value.strip()
        password = self.query_one("#connect-smb-password", Input).value
        config, secret_source = smb_config_and_secrets(
            server=server, share=share, port_text=port_text, username=username, password=password
        )
        store = await store_from_config(BackendKind.SMB, config, secret_source)
        return store, f"smb://{server}/{share}"

    # -- saved profiles (SavedProfileManager) -----------------------------
    # Thin @work wrappers: Textual's @work schedules against the actual
    # Screen it's defined on, so the entry points stay here even though
    # each body lives on self._profiles. busy=False throughout: local
    # profiles.json disk I/O, not network.

    @work(busy=False)
    async def _show_profile_name_row(self, backend: _ProfileBackend) -> None:
        await self._profiles.show_name_row(backend)

    # Run on mount and after every save/delete so both tabs' pickers stay
    # in sync with ``profiles.json``.
    @work(busy=False)
    async def refresh_profile_lists(self) -> None:
        await self._profiles.refresh_lists()

    @work(busy=False)
    async def _load_selected_profile(self, backend: _ProfileBackend, name: str) -> None:
        await self._profiles.load_selected(backend, name)

    @work(busy=False)
    async def _confirm_save_profile(self, backend: _ProfileBackend) -> None:
        await self._profiles.confirm_save(backend)

    # Immediate, no confirmation dialog — matches this app's only existing
    # precedent for a destructive action (WorklistScreen's ``x`` key
    # cancels a job immediately, no confirm step).
    @work(busy=False)
    async def _delete_selected_profile(self, backend: _ProfileBackend) -> None:
        await self._profiles.delete_selected(backend)

    # -- remote bucket/container browsing (RemoteOptionsBrowser) ----------
    # Same thin-wrapper reasoning as the profile methods -- the real busy
    # indicator lives inside RemoteOptionsBrowser.browse() itself.

    @work(busy=False)
    async def _browse_buckets(self) -> None:
        await self._remote_browser.browse_buckets()

    @work(busy=False)
    async def _browse_containers(self) -> None:
        await self._remote_browser.browse_containers()

    # -- scan --------------------------------------------------------------
    # One shared scan for every backend, "local" included: Session.discover()
    # is itself just LocalFsStore(path) fed into the same discover_remote()
    # internals -- no separate local-only scan path to keep in sync.
    # busy=False: this already reports its own progress into
    # #connect-status via on_progress below -- a generic DebouncedProgress
    # sink would fight with those writes.
    @work(busy=False)
    async def _scan(self, store: ObjectStore, label: str) -> None:
        status = self.query_one("#connect-status", Static)
        # repr() before safe(): the reverse order would have repr()
        # re-escape safe()'s own RTL-isolate marks as visible text.
        status.update(f"scanning {safe(repr(label))}...")
        session = cast(ApmRepoBrowserApp, self.app).session

        async def on_progress(p: Progress) -> None:
            found = p.found if p.found is not None else 0
            repos = pluralize(found, "repository", "repositories")
            status.update(f"scanning... found {found} {repos}")

        meter = ProgressMeter(callback=on_progress)
        repos: list[Repository] = []
        try:
            repos.extend([repo async for repo in session.discover_remote(store, progress=meter.update)])
        except asyncio.CancelledError:
            # Cancel button/Esc -- restore this dialog's editable state,
            # then re-raise so Textual's own Worker still marks itself
            # CANCELLED, not FAILED. Worker.cancel() only requests
            # cancellation, so for the Escape path this runs after
            # dismiss(None) already closed the screen; _end_scan's
            # NoMatches tolerance keeps that harmless.
            self._end_scan()
            raise
        except ApmRepoError as exc:
            self._fail_scan(exc)
            return
        except Exception as exc:  # a real network/auth failure (bad creds,
            # unreachable endpoint, wrong bucket/container/path) -- not an
            # ApmRepoError, since S3Store/AzureStore raise backend-specific
            # exceptions this dialog's construction step can't have caught.
            # An expected, common outcome, not a bug to crash the worker.
            self._fail_scan(exc)
            return
        if not repos:
            self._fail_scan(f"no repository found at {label!r}")
            return
        self.dismiss((repos, label))

    def _fail_scan(self, message: object) -> None:
        # Routed through show_error so every failure renders identically.
        # State reset goes through _end_scan, shared with the
        # cancellation path.
        self._end_scan()
        show_error(self, "#connect-status", message)
