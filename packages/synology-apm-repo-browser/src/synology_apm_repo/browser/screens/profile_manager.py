"""``SavedProfileManager``: ``ConnectDialog``'s saved-connection-profile
CRUD (list/load/save/delete), split out for the same reason
``goto_walker.py``'s ``GotoChainWalker`` is split out of ``UnitScreen``:
held privately, reaching back into the screen that owns it only through
the small public surface it exposes for this
(``validated_store_for``, ``refresh_profile_lists``, the ``naming_profile``
reactive, ``post_message``, plus Textual's own ``query_one``).

Also owns the S3/Azure/SMB backend vocabulary (``_ProfileBackend``) —
nothing outside profile persistence and ``ConnectDialog``'s own backend
dispatch needs it. The per-backend field table itself is owned by
``sdk.profiles`` (``ProfileFieldSpec``/``form_fields_for``), not this
module: it's shared, identically, by the CLI and TUI, so keeping it in
the SDK avoids each surface maintaining its own copy.
"""

from __future__ import annotations

import enum
from collections.abc import Mapping
from typing import TYPE_CHECKING

from textual.message import Message
from textual.widgets import Checkbox, Input, Select, Static

from synology_apm_repo.browser.screens._shared import show_error
from synology_apm_repo.browser.strings import CONNECT_PROFILE_NAME_REQUIRED_WARNING
from synology_apm_repo.sdk.presentation.markup import safe
from synology_apm_repo.sdk.profiles import (
    BackendKind,
    ProfileFieldSpec,
    ProfileSummary,
    delete_profile,
    form_fields_for,
    list_profiles,
    load_profile,
    save_profile,
)

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog


class _ProfileBackend(enum.StrEnum):
    """The three backends a saved connection profile can apply to."""

    S3 = "s3"
    AZURE = "azure"
    SMB = "smb"


def profile_backend_of(widget_id: str) -> _ProfileBackend:
    """Every profile-related widget id is prefixed ``connect-s3-``,
    ``connect-azure-``, or ``connect-smb-`` -- this recovers which tab a
    pressed button/submitted input belongs to without needing a second
    id->backend mapping."""
    for backend in _ProfileBackend:
        if widget_id.startswith(f"connect-{backend}"):
            return backend
    raise ValueError(f"widget id has no recognizable backend prefix: {widget_id!r}")  # pragma: no cover - defensive


_BACKEND_KIND: dict[_ProfileBackend, BackendKind] = {
    _ProfileBackend.S3: BackendKind.S3,
    _ProfileBackend.AZURE: BackendKind.AZURE,
    _ProfileBackend.SMB: BackendKind.SMB,
}


class SavedProfileManager:
    class ProfilesRefreshed(Message):
        """Posted after ``refresh_lists`` re-reads ``profiles.json`` --
        handled by ``ConnectDialog.on_saved_profile_manager_profiles_refreshed``,
        the sole place that writes the three per-backend ``Select``
        widgets this carries the data for."""

        def __init__(self, profiles: list[ProfileSummary]) -> None:
            self.profiles = profiles
            super().__init__()

    def __init__(self, dialog: ConnectDialog) -> None:
        self._dialog = dialog

    @staticmethod
    def _field_widget_id(prefix: str, field: ProfileFieldSpec) -> str:
        return f"#connect-{prefix}-{field.name.replace('_', '-')}"

    def fields(self, prefix: str, fields: tuple[ProfileFieldSpec, ...]) -> dict[str, str | bool]:
        """One tab's current field values, flattened into
        ``save_profile()``'s canonical field names — driven by ``fields``
        (``sdk.profiles.form_fields_for``) rather than a field-by-field
        method per tab."""
        result: dict[str, str | bool] = {}
        for field in fields:
            widget_id = self._field_widget_id(prefix, field)
            if field.is_checkbox:
                result[field.name] = self._dialog.query_one(widget_id, Checkbox).value
            else:
                value = self._dialog.query_one(widget_id, Input).value
                result[field.name] = value.strip() if field.strip else value
        return result

    def refill(self, prefix: str, fields: tuple[ProfileFieldSpec, ...], values: Mapping[str, str | bool | int]) -> None:
        for field in fields:
            widget_id = self._field_widget_id(prefix, field)
            if field.is_checkbox:
                self._dialog.query_one(widget_id, Checkbox).value = bool(values.get(field.name, True))
            else:
                self._dialog.query_one(widget_id, Input).value = str(values.get(field.name, ""))

    async def show_name_row(self, backend: _ProfileBackend) -> None:
        """ "Save as profile...": validates the tab's fields the same
        no-I/O way ``ConnectDialog._submit()`` does, to catch an
        obviously-missing required field before ever showing the name
        prompt — a successful live connection is never required."""
        if await self._dialog.validated_store_for(backend) is None:
            return
        self._dialog.naming_profile = backend

    def hide_name_row(self) -> None:
        self._dialog.naming_profile = None

    async def refresh_lists(self) -> None:
        try:
            profiles = await list_profiles()
        except Exception as exc:  # a corrupt config file or locked/missing keyring must never
            # crash this dialog -- manual field entry + Connect stays fully usable regardless,
            # same broad-catch rationale as ConnectDialog._scan()'s own.
            show_error(self._dialog, "#connect-status", exc)
            return
        self._dialog.post_message(self.ProfilesRefreshed(profiles))

    async def load_selected(self, backend: _ProfileBackend, name: str) -> None:
        try:
            fields = await load_profile(name)
        except Exception as exc:  # Same guard as refresh_lists above.
            show_error(self._dialog, "#connect-status", exc)
            return
        self.refill(backend, form_fields_for(_BACKEND_KIND[backend]), fields)

    async def confirm_save(self, backend: _ProfileBackend) -> None:
        name = self._dialog.query_one(f"#connect-{backend}-profile-name-input", Input).value.strip()
        status = self._dialog.query_one("#connect-status", Static)
        if not name:
            show_error(self._dialog, "#connect-status", CONNECT_PROFILE_NAME_REQUIRED_WARNING)
            return
        kind = _BACKEND_KIND[backend]
        fields = self.fields(backend, form_fields_for(kind))
        try:
            await save_profile(name, kind, fields)
        except Exception as exc:  # Same guard as refresh_lists above.
            show_error(self._dialog, "#connect-status", exc)
            return
        self.hide_name_row()
        # repr() (real-value quoting/escaping) runs before safe() (markup
        # escaping + RTL-isolate wrap) -- the reverse order would have
        # safe()'s isolate marks re-escaped as visible text by repr()'s
        # own non-printable-character handling.
        status.update(f"saved profile {safe(repr(name))}")
        self._dialog.refresh_profile_lists()

    async def delete_selected(self, backend: _ProfileBackend) -> None:
        value = self._dialog.query_one(f"#connect-{backend}-profile-select", Select).value
        if not isinstance(value, str):
            return
        status = self._dialog.query_one("#connect-status", Static)
        try:
            await delete_profile(value)
        except Exception as exc:  # Same guard as refresh_lists above.
            show_error(self._dialog, "#connect-status", exc)
            return
        # Same repr()-before-safe() ordering as confirm_save(), and for
        # the same reason.
        status.update(f"deleted profile {safe(repr(value))}")
        self._dialog.refresh_profile_lists()
