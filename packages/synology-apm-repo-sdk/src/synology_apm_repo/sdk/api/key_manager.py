"""``KeyManager``: the encryption-key state machine for one opened
``Repository``, part of the Repository Layer (see ``api/__init__.py``).
Owns
exactly the state ``KeyStatus`` depends on (``keys``/``key_verification``/
``encrypted``) and the pure ``KeyStatus`` resolution logic.
"""

from __future__ import annotations

import enum

from ..dedup.keys import KeyMaterial, KeyVerification
from ..errors import KeyMismatchError, KeyRequiredError
from ..storage.base import ObjectStore
from ..storage.layout import RepositoryLayout, key_probe_layout


class KeyStatus(enum.Enum):
    """The same four states the ``doctor`` command reports, as a proper
    type instead of a raw dict. ``NO_KEY_PROVIDED`` means "encrypted, no
    key tried yet" — except in the rare case the encryption probe itself
    couldn't tell, which shouldn't happen for a properly initialized
    repository."""

    NO_KEY_PROVIDED = "no_key_provided"
    NOT_ENCRYPTED = "not_encrypted"
    VERIFIED = "verified"
    INVALID = "invalid"


def _resolve_key_status(
    keys: KeyMaterial | None, key_verification: KeyVerification | None, encrypted: bool | None
) -> KeyStatus:
    """``KeyManager.status``'s branching, shared with ``record()``."""
    if keys is None:
        if encrypted is False:
            return KeyStatus.NOT_ENCRYPTED
        return KeyStatus.NO_KEY_PROVIDED  # confirmed encrypted, or the rare "couldn't tell"
    if keys.is_no_encryption:
        return KeyStatus.NOT_ENCRYPTED
    if key_verification is not None and key_verification.ok:
        return KeyStatus.VERIFIED
    return KeyStatus.INVALID


class KeyManager:
    """One ``Repository``'s own encryption-key state: the pure state
    itself, not the ``verify()``/``adopt()``/``record()`` orchestration
    around it (``Repository.set_key()``'s job)."""

    def __init__(
        self,
        keys: KeyMaterial | None,
        key_verification: KeyVerification | None,
        *,
        encrypted: bool | None = None,
    ) -> None:
        self._keys = keys
        self._key_verification = key_verification
        # Only meaningful when keys is None; resolved eagerly since
        # Session.discover()/Session.open() already probe encryption
        # status before any key is tried.
        self._encrypted = encrypted
        self._status = _resolve_key_status(keys, key_verification, encrypted)

    @property
    def keys(self) -> KeyMaterial | None:
        """``None`` until key material has actually been supplied — at
        construction, or via a later ``adopt()`` (a rejected attempt never
        reaches ``adopt()``)."""
        return self._keys

    @property
    def status(self) -> KeyStatus:
        """A plain, no-I/O property returning the precomputed ``KeyStatus``."""
        return self._status

    @property
    def verification(self) -> KeyVerification | None:
        """The GCM-unwrap verification result, or ``None`` when no key was
        ever provided — the detail behind ``status``'s ``INVALID``/
        ``VERIFIED``."""
        return self._key_verification

    @property
    def is_encrypted(self) -> bool | None:
        """Whether this repository is actually encrypted — ``True`` even
        when the key turns out to be wrong; see ``verification`` for that.
        ``None`` only when the underlying probe genuinely couldn't tell.
        """
        if self._status is KeyStatus.NOT_ENCRYPTED:
            return False
        if self._status is KeyStatus.NO_KEY_PROVIDED:
            return self._encrypted  # True (confirmed encrypted) or None (couldn't tell)
        return True  # VERIFIED or INVALID

    def require_verified(self) -> None:
        """Raise before any catalog I/O if this repository is *confirmed*
        encrypted and its key hasn't been verified yet. Not called by
        ``Repository.catalogs()``: those rows are plaintext, needing no
        key."""
        if self._status is KeyStatus.NO_KEY_PROVIDED and self.is_encrypted is True:
            raise KeyRequiredError("this repository is encrypted; call set_key() before browsing workloads/versions")
        if self._status is KeyStatus.INVALID:
            raise KeyMismatchError(
                "the key previously supplied for this repository was rejected; "
                "call set_key() with a valid key before browsing workloads/versions"
            )

    async def verify(
        self, store: ObjectStore, layout: RepositoryLayout, key_string: str
    ) -> tuple[KeyMaterial, KeyVerification]:
        """Builds ``KeyMaterial`` from ``key_string`` and verifies it
        against ``layout``'s own key-probe location. Pure — doesn't touch
        this manager's own state; ``Repository.set_key()`` decides what to
        do with the result."""
        keys = KeyMaterial.from_key_string(key_string)
        verification = await keys.verify(store, key_probe_layout(layout))
        return keys, verification

    def adopt(self, keys: KeyMaterial) -> None:
        """Commits ``keys`` as this manager's own key material immediately,
        so not-yet-opened catalogs pick it up on their own eventual first
        open. Leaves ``status``/``verification`` untouched — ``record()``
        updates those once the reopen sweep completes."""
        self._keys = keys

    def record(self, keys: KeyMaterial, verification: KeyVerification) -> None:
        """Records ``verification``'s outcome and recomputes ``status``.
        ``keys`` is the just-tried key material, not necessarily
        ``self.keys`` — a rejected key never reaches ``adopt()``, but
        ``status`` must still distinguish "never tried" from "tried and
        failed"."""
        self._key_verification = verification
        self._status = _resolve_key_status(keys, verification, self._encrypted)
