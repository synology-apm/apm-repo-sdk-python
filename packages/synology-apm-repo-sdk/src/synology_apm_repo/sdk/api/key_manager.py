"""``KeyManager``: one opened ``Repository``'s encryption-key state
(``keys``/``key_verification``/``encrypted``) and the ``KeyStatus`` it
resolves to.
"""

from __future__ import annotations

import enum

from ..dedup.keys import KeyMaterial, KeyVerification
from ..errors import KeyMismatchError, KeyRequiredError
from ..storage.base import ObjectStore
from ..storage.layout import RepositoryLayout, key_probe_layout


class KeyStatus(enum.Enum):
    """A repository's key state. ``NO_KEY_PROVIDED`` means encrypted (or,
    rarely, undeterminable) with no key tried yet; ``INVALID`` means the
    last key tried was rejected."""

    NO_KEY_PROVIDED = "no_key_provided"
    NOT_ENCRYPTED = "not_encrypted"
    VERIFIED = "verified"
    INVALID = "invalid"

    @property
    def label(self) -> str:
        """This status as a user reads it (``"key needed"``,
        ``"not encrypted"``, ``"key verified"`` or ``"invalid key"``)."""
        return _KEY_STATUS_LABELS[self]


_KEY_STATUS_LABELS = {
    KeyStatus.NO_KEY_PROVIDED: "key needed",
    KeyStatus.NOT_ENCRYPTED: "not encrypted",
    KeyStatus.VERIFIED: "key verified",
    KeyStatus.INVALID: "invalid key",
}


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
    """One ``Repository``'s encryption-key state. ``Repository.set_key()``
    sequences ``verify()``/``adopt()``/``record()``."""

    def __init__(
        self,
        keys: KeyMaterial | None,
        key_verification: KeyVerification | None,
        *,
        encrypted: bool | None = None,
    ) -> None:
        self._keys = keys
        self._key_verification = key_verification
        # Only meaningful when keys is None; Session probes it when a
        # repository is opened without a key.
        self._encrypted = encrypted
        self._status = _resolve_key_status(keys, key_verification, encrypted)

    @property
    def keys(self) -> KeyMaterial | None:
        """The adopted key material; ``None`` until a key is supplied at
        construction or ``adopt()``-ed. A rejected key is never adopted."""
        return self._keys

    @property
    def status(self) -> KeyStatus:
        """The current ``KeyStatus``; no I/O."""
        return self._status

    @property
    def verification(self) -> KeyVerification | None:
        """The GCM-unwrap verification result, or ``None`` when no key was
        ever provided — the detail behind ``status``'s ``INVALID``/
        ``VERIFIED``."""
        return self._key_verification

    @property
    def is_encrypted(self) -> bool | None:
        """Whether this repository is encrypted, ``True`` even when the key
        was rejected; ``None`` when the probe couldn't tell."""
        if self._status is KeyStatus.NOT_ENCRYPTED:
            return False
        if self._status is KeyStatus.NO_KEY_PROVIDED:
            return self._encrypted  # True (confirmed encrypted) or None (couldn't tell)
        return True  # VERIFIED or INVALID

    def require_verified(self) -> None:
        """Check, before workload/version I/O, that a confirmed-encrypted
        repository has a verified key.

        Raises:
            KeyRequiredError: The repository is encrypted and no key was
                supplied.
            KeyMismatchError: The last key supplied was rejected.
        """
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
        """``KeyMaterial`` from ``key_string``, verified against ``layout``'s
        key-probe location. Leaves this manager's state unchanged.

        Raises:
            KeyMaterialError: ``key_string`` is malformed.
            DataCorruptError: The repository's key record is unreadable.
        """
        keys = KeyMaterial.from_key_string(key_string)
        verification = await keys.verify(store, key_probe_layout(layout))
        return keys, verification

    def adopt(self, keys: KeyMaterial) -> None:
        """Make ``keys`` the key material catalogs open with from now on.
        ``status``/``verification`` change only on ``record()``."""
        self._keys = keys

    def record(self, keys: KeyMaterial, verification: KeyVerification) -> None:
        """Record ``verification`` and recompute ``status``. ``keys`` is the
        key just tried, adopted or not, so a rejected key still yields
        ``INVALID`` rather than ``NO_KEY_PROVIDED``."""
        self._key_verification = verification
        self._status = _resolve_key_status(keys, verification, self._encrypted)
