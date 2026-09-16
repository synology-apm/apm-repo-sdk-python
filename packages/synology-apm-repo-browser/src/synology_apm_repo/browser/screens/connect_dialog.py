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

Local browsing is ``DirsOnlyTree`` (see its own docstring for the
directory-only filtering and the two navigation gaps a plain
``DirectoryTree`` has no answer for) paired with a plain path ``Input``
that mirrors the tree's current selection and can also be typed into
directly.

Two collaborators, split out the same way ``goto_walker.py``'s
``GotoChainWalker`` is split out of ``UnitScreen`` (see that module's own
docstring for the convention): ``profile_manager.SavedProfileManager``
(saved-connection-profile CRUD) and ``remote_browser.RemoteOptionsBrowser``
(the bucket/container "Browse" flow). This dialog keeps backend
selection, store construction/validation, and the scan itself.
"""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import cast

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, DirectoryTree, Input, OptionList, Select, Static, Tab, Tabs, Tree

from synology_apm_repo.browser.app import ApmRepoBrowserApp
from synology_apm_repo.browser.screens._shared import modal_box_css, move_cursor_to_parent, show_error
from synology_apm_repo.browser.screens.profile_manager import SavedProfileManager, _ProfileBackend, profile_backend_of
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
    CONNECT_DELETE_PROFILE_LABEL,
    CONNECT_LOCAL_PROMPT,
    CONNECT_NO_BUCKET_WARNING,
    CONNECT_NO_CONTAINER_WARNING,
    CONNECT_NO_PATH_WARNING,
    CONNECT_NO_SERVER_WARNING,
    CONNECT_NO_SHARE_WARNING,
    CONNECT_PATH_NOT_A_DIRECTORY_WARNING,
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
    CONNECT_SMB_INVALID_PORT_WARNING,
    CONNECT_SMB_PASSWORD_PLACEHOLDER,
    CONNECT_SMB_PROFILE_SELECT_PROMPT,
    CONNECT_SMB_SERVER_PLACEHOLDER,
    CONNECT_SMB_SHARE_PLACEHOLDER,
    CONNECT_SMB_USERNAME_PLACEHOLDER,
    CONNECT_SUBMIT_LABEL,
    CONNECT_SUBMIT_LABEL_LOCAL,
)
from synology_apm_repo.browser.widgets.dirs_only_tree import DirsOnlyTree
from synology_apm_repo.sdk.api import Repository
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.presentation.format import pluralize
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.presentation.progress import Progress, ProgressMeter
from synology_apm_repo.sdk.profiles import (
    AzureProfileConfig,
    BackendKind,
    S3ProfileConfig,
    SmbProfileConfig,
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


#: What this dialog dismisses with on a successful scan: every repository
#: found, plus the same display label ``BrowseScreen._repo_label`` needs
#: (a real path string, or ``"s3://bucket"``/``"azure://container"``/
#: ``"smb://server/share"``) — ``None`` on Esc/cancel. Connections are
#: *not* fetched here — this dialog's job ends at "confirmed found, and
#: cheaply opened"; ``BrowseScreen`` loads each repository's connections lazily,
#: on demand (see its own module docstring for why).
ConnectResult = tuple[list[Repository], str]


class _ConnectValidationError(Exception):
    """A field this dialog itself can check before ever attempting a
    scan (empty path/bucket/container name, or a local path that isn't
    a directory) — never raised for anything that needs real I/O to
    detect."""


class ConnectDialog(ModalScreen[ConnectResult | None]):
    """See module docstring. ``ModalScreen`` truncates the App-level
    binding chain at itself, same as ``KeyDialog``/``ExportScreen``."""

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
        # directly here since ConnectDialog isn't one (see
        # move_cursor_to_parent's own docstring for what this does and
        # why; see DirsOnlyTree's own docstring for how going *up out
        # of the rooted directory* is a different thing, handled by its
        # synthetic ".." entry instead). Never reached while an Input
        # has focus — Input already claims backspace for character
        # deletion, which wins first regardless of this binding.
        Binding("backspace", "cursor_to_parent", "To parent", show=False),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._backend: _Backend = _Backend.LOCAL
        # Guards against a second submit firing mid-scan (e.g. an
        # impatient double Enter) — cleared on every failure path inside
        # _scan(); never cleared on success since the dialog dismisses
        # and there is nothing left here to guard.
        self._scanning = False
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
                # share" operation the way S3/Azure list buckets/containers
                # (see sdk.profiles.list_remote_items's own docstring), so
                # the share name is always typed directly.
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
            self._submit()
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
        self._switch_backend(cast(_Backend, event.tab.id))

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
            # See DirsOnlyTree's own docstring for why ".." can never
            # collide with a real directory's name.
            self.query_one("#connect-local-tree", DirsOnlyTree).path = str(event.path)

    def action_cursor_to_parent(self) -> None:
        if isinstance(self.focused, Tree):
            move_cursor_to_parent(self.focused)

    def _switch_backend(self, backend: _Backend) -> None:
        """Shows the given backend's field group, hides the others, and
        swaps the submit label."""
        # Deliberately leaves focus wherever it already is (on the tabs strip
        # in the common case) rather than moving it into the newly-active
        # group: this runs on every Tabs.TabActivated, including one fired by
        # left/right while the tabs strip has focus, and moving focus here
        # would land it on an Input/Tree after the very first arrow press --
        # silently ending arrow-key backend browsing one step in. Tab is what
        # moves focus from the tabs strip into the active group.
        self._profiles.hide_name_row()
        self._backend = backend
        for key in _Backend:
            self.query_one(f"#connect-{key}-fields").set_class(key == backend, "active")
        self.query_one("#connect-submit", Button).label = (
            CONNECT_SUBMIT_LABEL_LOCAL if backend == _Backend.LOCAL else CONNECT_SUBMIT_LABEL
        )

    def action_cancel(self) -> None:
        # Esc while the inline "save as profile" name row is open closes
        # only that row, not the whole dialog.
        if self._profiles.naming_profile is not None:
            self._profiles.hide_name_row()
            return
        self.dismiss(None)

    # -- store construction/validation -----------------------------------
    # Async ``@work`` (never ``thread=True``) — see browser/README.md. ``_build_store``
    # awaits ``store_from_config`` for the s3/azure backends, so this and
    # ``validated_store_for``'s own callers must run as workers rather than
    # plain synchronous event handlers.
    @work
    async def _submit(self) -> None:
        if self._scanning or any(self._remote_browser.browsing.values()):
            return
        result = await self._validated_store(self._build_store)
        if result is None:
            return
        store, label = result
        self._scanning = True
        self.query_one("#connect-submit", Button).disabled = True
        self._scan(store, label)

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
        except _ConnectValidationError as exc:
            # A field this dialog itself can check with no I/O (see that
            # exception's own docstring).
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
        browsing while a submit is already scanning."""
        return self._scanning

    async def _build_store(self) -> tuple[ObjectStore, str]:
        if self._backend == _Backend.LOCAL:
            return self._build_local_store()
        if self._backend == _Backend.S3:
            return await self._build_s3_store()
        if self._backend == _Backend.AZURE:
            return await self._build_azure_store()
        return await self._build_smb_store()

    def _build_local_store(self) -> tuple[ObjectStore, str]:
        raw = self.query_one("#connect-local-path", Input).value.strip()
        if not raw:
            raise _ConnectValidationError(CONNECT_NO_PATH_WARNING)
        path = Path(raw).expanduser()
        if not path.is_dir():
            raise _ConnectValidationError(CONNECT_PATH_NOT_A_DIRECTORY_WARNING)
        return LocalFsStore(path), str(path)

    def _s3_config_and_secrets(self, bucket: str) -> tuple[S3ProfileConfig, dict[str, str]]:
        """Every S3 tab field, split into ``store_from_config``'s
        ``config``/``secret_source`` pair — shared by ``_build_s3_store``
        (``bucket`` already chosen) and ``s3_client_kwargs``
        (``RemoteOptionsBrowser.browse_buckets``'s bucket-less
        placeholder)."""
        endpoint = self.query_one("#connect-s3-endpoint", Input).value.strip()
        region = self.query_one("#connect-s3-region", Input).value.strip()
        access_key = self.query_one("#connect-s3-access-key", Input).value.strip()
        secret_key = self.query_one("#connect-s3-secret-key", Input).value
        verify = self.query_one("#connect-s3-verify-tls", Checkbox).value

        config = S3ProfileConfig(bucket=bucket, endpoint=endpoint or None, region=region or None, verify_tls=verify)
        return config, {"access_key": access_key, "secret_key": secret_key}

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
        if not bucket:
            raise _ConnectValidationError(CONNECT_NO_BUCKET_WARNING)
        config, secret_source = self._s3_config_and_secrets(bucket)
        store = await store_from_config(BackendKind.S3, config, secret_source)
        return store, f"s3://{bucket}"

    def _azure_config_and_secrets(self, container: str) -> tuple[AzureProfileConfig, dict[str, str]]:
        """Every Azure tab field, split into ``store_from_config``'s
        ``config``/``secret_source`` pair — shared by ``_build_azure_store``
        (``container`` already chosen) and ``azure_client_kwargs``
        (``RemoteOptionsBrowser.browse_containers``'s container-less
        placeholder). This dialog has no ``verify_tls``-equivalent field
        for Azure (unlike S3's own checkbox), so ``AzureProfileConfig``'s
        default (``verify_tls=True``) always applies here, same as a
        saved Azure profile with that field never touched."""
        account_url = self.query_one("#connect-azure-account-url", Input).value.strip()
        credential = self.query_one("#connect-azure-credential", Input).value

        config = AzureProfileConfig(container=container, account_url=account_url or None)
        return config, {"credential": credential}

    def azure_client_kwargs(self) -> dict[str, object]:
        """The container-less resolved client kwargs
        ``RemoteOptionsBrowser.browse_containers`` needs — see
        ``s3_client_kwargs``'s own docstring for why this stays a
        separate call from ``_build_azure_store``."""
        config, secret_source = self._azure_config_and_secrets("")
        return client_kwargs_with_secrets(config, secret_source)

    async def _build_azure_store(self) -> tuple[ObjectStore, str]:
        container = self.query_one("#connect-azure-container", Input).value.strip()
        if not container:
            raise _ConnectValidationError(CONNECT_NO_CONTAINER_WARNING)
        config, secret_source = self._azure_config_and_secrets(container)
        store = await store_from_config(BackendKind.AZURE, config, secret_source)
        return store, f"azure://{container}"

    def _smb_config_and_secrets(self, server: str, share: str) -> tuple[SmbProfileConfig, dict[str, str]]:
        """Every SMB tab field, split into ``store_from_config``'s
        ``config``/``secret_source`` pair — same role as
        ``_s3_config_and_secrets``/``_azure_config_and_secrets``, but with
        no bucket-less/container-less counterpart: SMB has no "Browse"
        button (see ``compose()``'s own comment for why), so this is only
        ever called from ``_build_smb_store`` with both already chosen."""
        port_text = self.query_one("#connect-smb-port", Input).value.strip()
        username = self.query_one("#connect-smb-username", Input).value.strip()
        password = self.query_one("#connect-smb-password", Input).value

        try:
            port = int(port_text) if port_text else 445
        except ValueError:
            raise _ConnectValidationError(CONNECT_SMB_INVALID_PORT_WARNING) from None

        config = SmbProfileConfig(server=server, share=share, port=port, username=username or None)
        return config, {"password": password}

    async def _build_smb_store(self) -> tuple[ObjectStore, str]:
        server = self.query_one("#connect-smb-server", Input).value.strip()
        share = self.query_one("#connect-smb-share", Input).value.strip()
        if not server:
            raise _ConnectValidationError(CONNECT_NO_SERVER_WARNING)
        if not share:
            raise _ConnectValidationError(CONNECT_NO_SHARE_WARNING)
        config, secret_source = self._smb_config_and_secrets(server, share)
        store = await store_from_config(BackendKind.SMB, config, secret_source)
        return store, f"smb://{server}/{share}"

    # -- saved profiles (SavedProfileManager) -----------------------------
    # Thin @work wrappers: Textual's @work decorator schedules against the
    # actual Screen/Widget it's defined on, so the worker entry points stay
    # here even though each body now lives on self._profiles.

    @work
    async def _show_profile_name_row(self, backend: _ProfileBackend) -> None:
        await self._profiles.show_name_row(backend)

    # Run on mount and after every save/delete so both tabs' pickers stay
    # in sync with ``profiles.json``.
    @work
    async def refresh_profile_lists(self) -> None:
        await self._profiles.refresh_lists()

    @work
    async def _load_selected_profile(self, backend: _ProfileBackend, name: str) -> None:
        await self._profiles.load_selected(backend, name)

    @work
    async def _confirm_save_profile(self, backend: _ProfileBackend) -> None:
        await self._profiles.confirm_save(backend)

    # Immediate, no confirmation dialog — matches this app's only existing
    # precedent for a destructive action (WorklistScreen's ``x`` key
    # cancels a job immediately, no confirm step).
    @work
    async def _delete_selected_profile(self, backend: _ProfileBackend) -> None:
        await self._profiles.delete_selected(backend)

    # -- remote bucket/container browsing (RemoteOptionsBrowser) ----------
    # Same thin-wrapper reasoning as the profile methods above.

    @work
    async def _browse_buckets(self) -> None:
        await self._remote_browser.browse_buckets()

    @work
    async def _browse_containers(self) -> None:
        await self._remote_browser.browse_containers()

    # -- scan --------------------------------------------------------------
    # Async ``@work`` (never ``thread=True``) — see browser/README.md. One shared
    # scan for every backend, "local" included: ``store`` is already fully
    # backend-specific by the time this runs, and ``Session.discover()`` is
    # itself defined as nothing more than ``LocalFsStore(path)`` fed into these
    # same ``discover_remote()``-shared internals — so there's no separate
    # local-only scan path to keep in sync with this one.
    @work
    async def _scan(self, store: ObjectStore, label: str) -> None:
        status = self.query_one("#connect-status", Static)
        status.update(f"scanning {safe(label)!r}...")
        session = cast(ApmRepoBrowserApp, self.app).session

        async def on_progress(p: Progress) -> None:
            found = p.found if p.found is not None else 0
            repos = pluralize(found, "repository", "repositories")
            status.update(f"scanning... found {found} {repos}")

        meter = ProgressMeter(callback=on_progress)
        repos: list[Repository] = []
        try:
            repos.extend([repo async for repo in session.discover_remote(store, progress=meter.update)])
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
        # failure renders identically.
        self._scanning = False
        self.query_one("#connect-submit", Button).disabled = False
        show_error(self, "#connect-status", message)
