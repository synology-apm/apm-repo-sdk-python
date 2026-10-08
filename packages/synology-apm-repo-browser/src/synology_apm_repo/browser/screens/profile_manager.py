"""``SavedProfileManager``: ``ConnectDialog``'s saved connection profiles
(list, load, save, delete). It reaches the dialog only through
``validated_store_for``, ``refresh_profile_lists``, the ``naming_profile``
reactive and ``query_one``/``post_message``; the per-backend fields come from
``sdk.profiles.form_fields_for``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from textual.message import Message
from textual.widgets import Checkbox, Input, Select, Static

from synology_apm_repo.browser.screens._shared import show_error
from synology_apm_repo.browser.strings import CONNECT_PROFILE_NAME_REQUIRED_WARNING
from synology_apm_repo.sdk.presentation import safe
from synology_apm_repo.sdk.profiles import (
    BackendKind,
    Profile,
    ProfileFieldSpec,
    delete_profile,
    form_fields_for,
    list_profiles,
    profile_fields_with_secrets,
    save_profile,
)

if TYPE_CHECKING:
    from synology_apm_repo.browser.screens.connect_dialog import ConnectDialog


def profile_backend_of(widget_id: str) -> BackendKind:
    """The backend a profile widget belongs to, from its
    ``connect-<backend>-`` id prefix."""
    for backend in BackendKind:
        if widget_id.startswith(f"connect-{backend}"):
            return backend
    raise ValueError(f"widget id has no recognizable backend prefix: {widget_id!r}")  # pragma: no cover - defensive


class SavedProfileManager:
    class ProfilesRefreshed(Message):
        """Posted after ``refresh_lists`` re-reads the saved profiles;
        ``ConnectDialog`` fills its per-backend ``Select`` widgets from it."""

        def __init__(self, profiles: list[Profile]) -> None:
            self.profiles = profiles
            super().__init__()

    def __init__(self, dialog: ConnectDialog) -> None:
        self._dialog = dialog

    @staticmethod
    def _field_widget_id(backend: BackendKind, field: ProfileFieldSpec) -> str:
        return f"#connect-{backend}-{field.name.replace('_', '-')}"

    def fields(self, backend: BackendKind) -> dict[str, str | bool]:
        """One tab's current field values, in ``save_profile()``'s canonical
        field names, read through ``form_fields_for(backend)``."""
        result: dict[str, str | bool] = {}
        for field in form_fields_for(backend):
            widget_id = self._field_widget_id(backend, field)
            if field.is_checkbox:
                result[field.name] = self._dialog.query_one(widget_id, Checkbox).value
            else:
                value = self._dialog.query_one(widget_id, Input).value
                result[field.name] = value.strip() if field.strip else value
        return result

    def refill(self, backend: BackendKind, values: Mapping[str, str | bool | int]) -> None:
        for field in form_fields_for(backend):
            widget_id = self._field_widget_id(backend, field)
            if field.is_checkbox:
                self._dialog.query_one(widget_id, Checkbox).value = bool(values.get(field.name, True))
            else:
                self._dialog.query_one(widget_id, Input).value = str(values.get(field.name, ""))

    async def show_name_row(self, backend: BackendKind) -> None:
        """ "Save as profile...": shows the name prompt once the tab's fields
        pass ``ConnectDialog``'s validation (no connection is attempted)."""
        if await self._dialog.validated_store_for(backend) is None:
            return
        self._dialog.naming_profile = backend

    def hide_name_row(self) -> None:
        self._dialog.naming_profile = None

    async def refresh_lists(self) -> None:
        try:
            profiles = await list_profiles()
        except Exception as exc:  # noqa: BLE001 - a corrupt profiles.json must not crash the
            # dialog: manual entry still works.
            show_error(self._dialog, "#connect-status", exc)
            return
        self._dialog.post_message(self.ProfilesRefreshed(profiles))

    async def load_selected(self, backend: BackendKind, name: str) -> None:
        try:
            fields = await profile_fields_with_secrets(name)
        except Exception as exc:  # noqa: BLE001 - as in refresh_lists, or an unavailable keyring
            show_error(self._dialog, "#connect-status", exc)
            return
        self.refill(backend, fields)

    async def confirm_save(self, backend: BackendKind) -> None:
        name = self._dialog.query_one(f"#connect-{backend}-profile-name-input", Input).value.strip()
        status = self._dialog.query_one("#connect-status", Static)
        if not name:
            show_error(self._dialog, "#connect-status", CONNECT_PROFILE_NAME_REQUIRED_WARNING)
            return
        fields = self.fields(backend)
        try:
            await save_profile(name, backend, fields)
        except Exception as exc:  # noqa: BLE001 - as in refresh_lists, or an unavailable keyring
            show_error(self._dialog, "#connect-status", exc)
            return
        self.hide_name_row()
        # repr() before safe(), so repr() doesn't escape safe()'s isolate marks.
        status.update(f"saved profile {safe(repr(name))}")
        self._dialog.refresh_profile_lists()

    async def delete_selected(self, backend: BackendKind) -> None:
        value = self._dialog.query_one(f"#connect-{backend}-profile-select", Select).value
        if not isinstance(value, str):
            return
        status = self._dialog.query_one("#connect-status", Static)
        try:
            await delete_profile(value)
        except Exception as exc:  # noqa: BLE001 - as in refresh_lists, or an unavailable keyring
            show_error(self._dialog, "#connect-status", exc)
            return
        # repr() before safe(), as in confirm_save().
        status.update(f"deleted profile {safe(repr(value))}")
        self._dialog.refresh_profile_lists()
