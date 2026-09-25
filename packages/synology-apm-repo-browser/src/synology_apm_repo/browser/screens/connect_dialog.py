"""``ConnectDialog`` is a small, centered modal overlay -- same treatment
as ``KeyDialog``/``ExportScreen`` -- for picking *where* to browse: a
local directory, an S3/Azure Blob Storage-backed repository, or an SMB
share. Auto-opened once on app start (see ``app.py``'s own ``on_mount``);
``c`` reopens it later from ``BrowseScreen`` the same way. This is the
only place a source is ever picked -- ``BrowseScreen`` has no path field
of its own.

The connection test/repository scan runs here, not in ``BrowseScreen``:
``_scan`` drives ``Session.discover_remote`` for every backend, streaming
"found N so far" progress into ``#connect-status``, and dismisses only
once at least one repository was actually found -- a construction-time
validation error, a connectivity failure, or an empty scan all surface
their message right here, inline, letting the user fix and retry without
leaving the dialog. By the time ``BrowseScreen`` gets control back, there
is always at least one already-scanned repository to show.

Local browsing is ``DirsOnlyTree`` -- filters to directories only, and
adds two navigation conveniences a plain ``DirectoryTree`` has no
built-in answer for: a synthetic ``".."`` entry for going up out of the
rooted directory, and type-ahead that jumps the cursor to the first
sibling starting with what's been typed -- paired with a plain path
``Input`` that mirrors the tree's current selection and can also be
typed into directly.

Two collaborators, held privately and reaching back into this dialog
only through the small public surface exposed for that purpose -- the
same convention ``goto_walker.py``'s ``GotoChainWalker`` uses to reach
back into ``UnitScreen``: ``profile_manager.SavedProfileManager``
(saved-connection-profile CRUD) and ``remote_browser.RemoteOptionsBrowser``
(the bucket/container "Browse" flow). This dialog keeps backend
selection, store construction/validation, and the scan itself.
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
#: flight -- the backend-picker tabs plus all four fields groups
#: (disabling a container recursively disables every focusable
#: descendant, so this doesn't need each individual ``Input``/``Tree``/
#: ``Select``/``Checkbox``/profile button spelled out). Deliberately
#: excludes ``#connect-actions``: that's the submit/cancel button itself,
#: which must stay clickable throughout -- it's what actually cancels
#: the scan.
_DISABLE_WHILE_SCANNING_IDS = (
    "connect-backend-tabs",
    "connect-local-fields",
    "connect-smb-fields",
    "connect-s3-fields",
    "connect-azure-fields",
)


#: What this dialog dismisses with on a successful scan: every repository
#: found, plus the same display label ``BrowseScreen._repo_label`` needs
#: (a real path string, or ``"s3://bucket"``/``"azure://container"``/
#: ``"smb://server/share"``) — ``None`` on Esc/cancel. Connections are
#: *not* fetched here — this dialog's job ends at "confirmed found, and
#: cheaply opened"; ``BrowseScreen`` loads each repository's connections
#: lazily, on demand, only once its catalog node is actually expanded --
#: fetching from S3/Azure is a real network cost (several full
#: SQLite-file downloads), so connecting every discovered repository
#: eagerly would mean waiting on repositories the user may never look at.
ConnectResult = tuple[list[Repository], str]


class ConnectDialog(ModalScreen[ConnectResult | None]):
    """``ModalScreen`` truncates the App-level binding chain at itself,
    same as ``KeyDialog``/``ExportScreen``."""

    #: Which backend tab is active. ``init=False``: the real initial value
    #: is established by the ``Tabs`` widget's own first ``TabActivated``
    #: (fired once it mounts with its default active tab), not by this
    #: reactive auto-firing its watcher a second time at construction.
    backend: reactive[_Backend] = reactive(_Backend.LOCAL, init=False)

    #: Which tab's inline "save as profile" name row is open, if any --
    #: Esc while it's open closes only that row, not the whole dialog (see
    #: action_cancel). ``init=False``: nothing is open at construction, so
    #: there's nothing for the watcher to do on the first fire.
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
        # Same shared "jump to parent, collapse it" behavior every
        # other tree in the app gets via NavigableScreen — reached
        # directly here since ConnectDialog isn't one. Going *up out of*
        # the rooted local directory is a different thing entirely --
        # DirsOnlyTree handles that itself via a synthetic ".." leaf on
        # its own root, which re-roots the whole tree at the parent
        # directory rather than just moving the cursor. Never reached while an Input
        # has focus — Input already claims backspace for character
        # deletion, which wins first regardless of this binding.
        Binding("backspace", "cursor_to_parent", "To parent", show=False),
    ]

    def __init__(self) -> None:
        super().__init__()
        # The real Worker _scan() is currently running as -- None
        # whenever no scan is in flight, which also doubles as the
        # ``scanning`` property's flag: a guard against a second submit
        # firing mid-scan, and against ``RemoteOptionsBrowser.browse()``
        # kicking off a bucket/container listing while a scan is already
        # running. The two are set/cleared in lockstep at every site below, so tracking
        # them as one field removes a whole class of desync bug a
        # separate bool could invite. Textual's own @work decorator
        # returns this synchronously (scheduling, not awaiting, the
        # coroutine), so there's no race between _submit() assigning it
        # and a user pressing Cancel a moment later. Never cleared on a
        # successful scan since the dialog dismisses and there is
        # nothing left here to guard.
        self._scan_worker: Worker[None] | None = None
        self._profiles = SavedProfileManager(self)
        self._remote_browser = RemoteOptionsBrowser(self)

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(CONNECT_PROMPT)
            # A single Tabs strip, not three separate Buttons: one Tab-stop
            # for this whole choice, with left/right immediately switching
            # the active backend (Tabs.TabActivated below) — Textual's
            # built-in equivalent of a segmented control, rather than
            # consuming three Tab-stops for what is really one selection.
            # Tab ids double as the ``_Backend`` literal values themselves,
            # so there's no separate id->backend mapping to maintain.
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
                # select_on_focus=False: Input's own default is True (select
                # the whole value on focus), which turns the very first
                # keystroke of an edit into "replace everything" instead of
                # "insert/delete at the cursor" — exactly wrong for a field
                # that already holds a real, usually-mostly-right path the
                # user wants to tweak, not retype from scratch.
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
                # No "prefix" field: ActiveProtect's own object-storage provisioning
                # always writes its marker directory (@ActiveProtectData) as the
                # bucket's first path segment, never under an admin-chosen sub-path
                # -- there is no real repository to reach by scoping narrower than
                # the bucket root itself (same reasoning applies to the Azure
                # container field below).
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
                # Account URL/credential first, container last: the
                # "Browse" button builds its client from whatever's
                # already filled in above it, so the fields it depends on
                # come before the field it populates — same ordering
                # rationale as the S3 tab's own credentials-before-bucket
                # layout above.
                #
                # No "connection string" field: AzureStore builds its client via the
                # real BlobServiceClient(**client_kwargs) constructor, not the separate
                # from_connection_string() classmethod, so only account_url/credential
                # are offered here, matching what that code path actually supports.
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
        # Keeps the typable Input in sync with whatever the tree
        # selected (a real subdirectory, or the synthetic ".." entry
        # alike — DirEntry.path already resolves ".." to the real
        # parent directory, so this needs no special-casing here), so
        # either interaction path (browse, or type directly) always
        # leaves the same field holding the truth _build_local_store()
        # reads from.
        self.query_one("#connect-local-path", Input).value = str(event.path)
        if str(event.node.label) == "..":
            # The one thing that *does* need special-casing: re-root
            # the whole tree at the parent instead of leaving ".." as
            # an inert selected leaf — every real subdirectory keeps
            # Tree's own normal expand-in-place behavior, untouched.
            # No real directory can ever be named exactly "..": every
            # filesystem reserves it as the parent-directory alias, so
            # this check can never misfire against a real subdirectory.
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
        # ``OptionList.highlighted`` starts ``None`` -- Enter's own ``action_select``
        # is a no-op with nothing highlighted, so the first option is
        # highlighted explicitly rather than leaving Enter dead until an
        # arrow key is pressed first.
        option_list.highlighted = 0
        option_list.add_class("-visible")
        option_list.focus()
        found = len(message.items)
        self.query_one("#connect-status", Static).update(f"found {found} {pluralize(found, message.noun)} — pick one")

    def _watch_backend(self, backend: _Backend) -> None:
        # Deliberately leaves focus wherever it already is (on the tabs strip
        # in the common case) rather than moving it into the newly-active
        # group: this runs on every Tabs.TabActivated, including one fired by
        # left/right while the tabs strip has focus, and moving focus here
        # would land it on an Input/Tree after the very first arrow press --
        # silently ending arrow-key backend browsing one step in. Tab is what
        # moves focus from the tabs strip into the active group.
        self._profiles.hide_name_row()
        for key in _Backend:
            self.query_one(f"#connect-{key}-fields").set_class(key == backend, "active")
        # Never reached mid-scan -- the tabs strip is one of the widgets
        # _set_fields_disabled() disables while a scan is in flight, so
        # this can't race with the button already showing
        # CONNECT_CANCEL_LABEL.
        self.query_one("#connect-submit", Button).label = self._submit_label()

    def _submit_label(self) -> str:
        return CONNECT_SUBMIT_LABEL_LOCAL if self.backend == _Backend.LOCAL else CONNECT_SUBMIT_LABEL

    def _set_fields_disabled(self, disabled: bool) -> None:
        """Toggles every backend-picker/field-group widget's own
        ``disabled`` -- each covers a whole container, so every
        focusable descendant (``Input``/``Tree``/``Select``/
        ``Checkbox``/profile button) is covered without spelling out
        each one -- while a scan is in flight, so nothing here can be
        edited out from under it. ``#connect-actions`` is deliberately
        not among them -- it must stay clickable to cancel the scan."""
        for widget_id in _DISABLE_WHILE_SCANNING_IDS:
            self.query_one(f"#{widget_id}").disabled = disabled

    def _end_scan(self) -> None:
        """Restores this dialog to its normal, editable state once a
        scan stops running -- however it stopped (a failure, or a
        cancellation; a *successful* scan dismisses the whole screen
        instead of ever reaching here). Shared by ``_fail_scan`` and
        ``_cancel_scan``/``action_cancel``'s own cancellation path, so
        neither leaves a different remnant of the other behind.
        Tolerates the screen already being gone (``NoMatches``): a
        cancellation's own cleanup can run after ``action_cancel``
        already dismissed the screen, since ``Worker.cancel()`` only
        *requests* cancellation -- the real ``CancelledError`` lands in
        ``_scan`` later, asynchronously, so for the Escape path this
        cleanup runs after the screen is already gone."""
        self._scan_worker = None
        with contextlib.suppress(NoMatches):
            self._set_fields_disabled(False)
            self.query_one("#connect-submit", Button).label = self._submit_label()
            # Clears whatever scan-in-progress text (the real "scanning
            # '...'..." line, or _cancel_scan's own "cancelling..." below)
            # is still showing -- _fail_scan's show_error() immediately
            # overwrites this with the real error right after, so a
            # failure never visibly flickers through a blank status.
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
        # it -- otherwise it kept running orphaned in the background
        # (still doing real network I/O for a remote backend) until it
        # finished on its own, wasting the work and risking a stray
        # dismiss()/#connect-status write against an already-gone screen.
        self._cancel_scan()
        self.dismiss(None)

    # -- store construction/validation -----------------------------------
    # ``_build_store`` awaits ``store_from_config`` for the s3/azure backends, so this and
    # ``validated_store_for``'s own callers must run as workers rather than
    # plain synchronous event handlers.
    # busy=False: this dialog is a ModalScreen, not a NavigableScreen, so
    # it has no breadcrumb to animate; constructing a store never performs
    # network I/O either way, so there's nothing here worth a busy
    # indicator anyway -- the real scan (_scan, below) already reports
    # its own progress.
    @work(busy=False)
    async def _submit(self) -> None:
        if self.scanning or any(self._remote_browser.browsing.values()):
            return
        result = await self._validated_store(self._build_store)
        if result is None:
            return
        store, label = result
        self._set_fields_disabled(True)
        # Stays enabled, not disabled -- it's now the Cancel button
        # (on_button_pressed branches on self.scanning), the only
        # widget in this whole dialog still meant to be clickable while
        # a scan is running.
        self.query_one("#connect-submit", Button).label = CONNECT_CANCEL_LABEL
        self._scan_worker = self._scan(store, label)

    async def _validated_store(
        self, build: Callable[[], Awaitable[tuple[ObjectStore, str]]]
    ) -> tuple[ObjectStore, str] | None:
        """Runs ``build`` (``_build_store``/``_build_s3_store``/
        ``_build_azure_store``), translating a validation failure into
        ``show_error`` and returning ``None`` — shared by ``_submit`` (uses
        the built store) and ``validated_store_for`` (only wants the
        validation, discards the store), since both need the identical
        two-except shape below."""
        try:
            return await build()
        except ConnectValidationError as exc:
            # A field this dialog itself can check before ever attempting
            # a scan (empty path/bucket/container/server/share, a local
            # path that isn't a directory, a non-numeric SMB port) -- no
            # I/O involved.
            show_error(self, "#connect-status", exc)
            return None
        except Exception as exc:  # Constructing a store never performs network I/O: S3Store
            # defers building its real client until the first read/listdir call, while
            # AzureStore builds its client object eagerly (still without a network round
            # trip) and so can raise a synchronous ValueError here for a malformed
            # account URL or an unresolvable account name — same broad-catch rationale
            # as _scan()'s/RemoteOptionsBrowser.browse()'s own: an expected, common
            # construction-time failure here, not a bug to crash over.
            show_error(self, "#connect-status", exc)
            return None

    async def validated_store_for(self, backend: _ProfileBackend) -> tuple[ObjectStore, str] | None:
        """Same validation ``_submit()`` runs before scanning, for one
        named backend rather than whichever tab is currently active —
        ``SavedProfileManager.show_name_row`` uses this to catch an
        obviously-missing required field before ever showing the "save
        as profile" name prompt; a successful live connection is never
        required for that."""
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
        flight — ``RemoteOptionsBrowser.browse()``'s own guard against
        browsing while a submit is already scanning, and ``_submit``'s
        own guard against a second submit firing mid-scan."""
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
        """Reads every S3 tab field and delegates the actual
        config/secret-source construction to ``core/connect/validate.py``'s
        pure ``s3_config_and_secrets`` — this method's own job is purely
        the Textual-coupled field reads; ``bucket`` decides whether the
        caller wants ``s3_client_kwargs``'s bucket-less placeholder or
        ``_build_s3_store``'s own already-chosen bucket, neither of
        which validates ``bucket`` here (``validate_s3`` does, for the
        latter)."""
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
        ``RemoteOptionsBrowser.browse_buckets`` needs — built via
        ``client_kwargs_with_secrets`` directly since
        ``RemoteOptionsBrowser.browse`` calls ``list_remote_items`` with
        already-resolved kwargs, not a ``config``/``secret_source``
        pair."""
        config, secret_source = self._s3_config_and_secrets("")
        return client_kwargs_with_secrets(config, secret_source)

    async def _build_s3_store(self) -> tuple[ObjectStore, str]:
        bucket = self.query_one("#connect-s3-bucket", Input).value.strip()
        # validate_s3 re-derives config/secret_source from the same
        # fields _s3_config_and_secrets above reads -- the bucket-check
        # is the only thing this needs beyond what that method already
        # gives s3_client_kwargs, and duplicating the read here keeps
        # the pure validate_s3/s3_config_and_secrets split in
        # core/connect/validate.py the single source of truth for what
        # "valid" means, rather than this method re-deriving it by hand.
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
        """Same role as ``_s3_config_and_secrets`` — the Textual-coupled
        field reads, delegating construction to
        ``core/connect/validate.py``'s pure ``azure_config_and_secrets``."""
        return azure_config_and_secrets(
            container=container,
            account_url=self.query_one("#connect-azure-account-url", Input).value.strip(),
            credential=self.query_one("#connect-azure-credential", Input).value,
        )

    def azure_client_kwargs(self) -> dict[str, object]:
        """The container-less resolved client kwargs
        ``RemoteOptionsBrowser.browse_containers`` needs -- built via
        ``client_kwargs_with_secrets`` directly, since
        ``RemoteOptionsBrowser.browse`` calls ``list_remote_items`` with
        already-resolved kwargs rather than the ``config``/``secret_source``
        pair ``_build_azure_store`` produces."""
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
    # Thin @work wrappers: Textual's @work decorator schedules against the
    # actual Screen/Widget it's defined on, so the worker entry points stay
    # here even though each body now lives on self._profiles. busy=False
    # throughout: local profiles.json disk I/O, not real network -- no
    # busy indicator needed, and this dialog -- a ModalScreen, not a
    # NavigableScreen -- has no breadcrumb to animate anyway.

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
    # Same thin-wrapper reasoning as the profile methods above -- the real
    # busy indicator lives inside RemoteOptionsBrowser.browse() itself,
    # which already owns "#connect-status".

    @work(busy=False)
    async def _browse_buckets(self) -> None:
        await self._remote_browser.browse_buckets()

    @work(busy=False)
    async def _browse_containers(self) -> None:
        await self._remote_browser.browse_containers()

    # -- scan --------------------------------------------------------------
    # One shared scan for every backend, "local" included: ``store`` is already fully
    # backend-specific by the time this runs, and ``Session.discover()`` is
    # itself defined as nothing more than ``LocalFsStore(path)`` fed into these
    # same ``discover_remote()``-shared internals — so there's no separate
    # local-only scan path to keep in sync with this one.
    # busy=False: this already reports its own real, continuously-updated
    # progress into #connect-status via on_progress below -- a generic
    # DebouncedProgress sink writing "<frame> Loading" into the same
    # widget on its own timer would fight with those writes instead of
    # complementing them, unlike every other call site in this package
    # where nothing else is already updating the target widget.
    @work(busy=False)
    async def _scan(self, store: ObjectStore, label: str) -> None:
        status = self.query_one("#connect-status", Static)
        # repr() before safe(), same as profile_manager.py's confirm_save()
        # -- the reverse order would have repr() re-escape safe()'s own
        # RTL-isolate marks as visible text.
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
            # Cancel button/Esc (action_cancel/_cancel_scan) -- restore
            # this dialog's own editable state, then re-raise so
            # Textual's own Worker still sees the cancellation and marks
            # itself CANCELLED, not FAILED. Worker.cancel() only
            # *requests* cancellation -- the real CancelledError lands
            # here later, asynchronously, so for the Escape path this
            # runs *after* action_cancel's own dismiss(None) already
            # closed the screen; _end_scan's own NoMatches tolerance is
            # what keeps that ordering harmless.
            self._end_scan()
            raise
        except ApmRepoError as exc:
            self._fail_scan(exc)
            return
        except Exception as exc:  # a real network/auth failure (bad creds, unreachable endpoint,
            # wrong bucket/container/path) isn't an ApmRepoError -- S3Store/AzureStore's own client
            # raises backend-specific exceptions (botocore's ClientError, azure-core's *Error, aiohttp
            # connection errors, ...) that this dialog's own construction step can't have caught (only
            # real I/O, which starts here, detects these). Broader than the ApmRepoError branch above
            # on purpose: a connection attempt failing this way is an expected, common outcome here,
            # not a bug to let crash the worker.
            self._fail_scan(exc)
            return
        if not repos:
            self._fail_scan(f"no repository found at {label!r}")
            return
        self.dismiss((repos, label))

    def _fail_scan(self, message: object) -> None:
        # Routed through show_error, rather than each call site
        # pre-formatting "[red]error:[/red] ..." itself, so every
        # failure renders identically. State reset itself goes through
        # _end_scan -- shared with the cancellation path below, so
        # neither leaves a different remnant of the other behind.
        self._end_scan()
        show_error(self, "#connect-status", message)
