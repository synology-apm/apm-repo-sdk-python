"""``KeyManager``: the encryption-key state machine for one opened
``Repository``, part of the Repository Layer (the ``Session``/
``Repository``/``Catalog`` split CLI/TUI code imports directly). Owns
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
    type instead of a raw dict.

    ``NOT_ENCRYPTED``/``VERIFIED``/``INVALID`` are only reachable once this
    repository is *known* to be encrypted (or known not to be) — never a guess.
    ``NO_KEY_PROVIDED`` means "encrypted, no key tried yet", not "unknown
    either way" — ``Session.discover``/``Session.open`` already resolve that
    up front for every repository opened without a key (see
    ``KeyManager.is_encrypted``). The one case ``NO_KEY_PROVIDED`` can
    still mean "genuinely couldn't tell" is that resolution itself coming
    back ``None`` — the repository's own encryption-key record being entirely
    absent, which shouldn't happen for a properly initialized repository."""

    NO_KEY_PROVIDED = "no_key_provided"
    NOT_ENCRYPTED = "not_encrypted"
    VERIFIED = "verified"
    INVALID = "invalid"


def _resolve_key_status(
    keys: KeyMaterial | None, key_verification: KeyVerification | None, encrypted: bool | None
) -> KeyStatus:
    """The one place ``KeyManager.status``'s branching lives; ``KeyManager.
    is_encrypted`` derives its own answer from the resulting ``KeyStatus``
    rather than repeating this branching, falling back to reading
    ``encrypted`` directly only for ``NO_KEY_PROVIDED`` (see
    ``is_encrypted``)."""
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
        # Only meaningful when ``keys`` is None (once a real key has been
        # tried, ``keys.is_no_encryption``/``key_verification`` alone already
        # fully answer both ``status`` and ``is_encrypted``). Resolved
        # eagerly here because Session.discover()/Session.open() already
        # probe encryption status up front, before any key is tried, and
        # hand the result in at construction time.
        self._encrypted = encrypted
        # Resolved once here (and again in record()) rather than
        # recomputed by both status and is_encrypted independently: both
        # would otherwise separately branch on keys/key_verification/
        # encrypted, so this precomputes the single KeyStatus both read
        # instead.
        self._status = _resolve_key_status(keys, key_verification, encrypted)

    @property
    def keys(self) -> KeyMaterial | None:
        """Read by ``Repository._open_catalog_resources`` to open/reopen each
        catalog's own ``DedupRepo`` under whichever key is currently
        adopted. ``None`` until key material has actually been supplied —
        at construction, or via a later ``adopt()`` (only reached once a
        ``set_key()`` attempt verifies ``ok``; a rejected attempt never
        touches this)."""
        return self._keys

    @property
    def status(self) -> KeyStatus:
        """A plain, no-I/O property returning the precomputed ``KeyStatus``."""
        return self._status

    @property
    def verification(self) -> KeyVerification | None:
        """The GCM-unwrap verification result, or ``None`` when no key
        was ever provided. ``status`` collapses this to one of four
        coarse states; this is the detail behind
        ``INVALID``/``VERIFIED`` (``gcm_ok``) — and, since GCM tag success is
        already cryptographic proof the key unwraps this repository's one
        wrapped-VaultKey record (which never changes after first
        initialization), already the whole answer to "is this key correct"
        on its own."""
        return self._key_verification

    @property
    def is_encrypted(self) -> bool | None:
        """Whether this repository is actually encrypted — a plain, no-I/O
        property, resolved from ``encrypted`` (see ``KeyStatus``) or, once
        a key has been tried, ``not keys.is_no_encryption``.
        ``True`` even when the key turns out to be wrong — "is encrypted"
        and "is the key correct" are different questions; see
        ``verification`` for the latter. ``None`` only when the
        underlying probe genuinely couldn't tell (the repository's own
        encryption-key record entirely absent) — never once a key has
        been tried.
        """
        if self._status is KeyStatus.NOT_ENCRYPTED:
            return False
        if self._status is KeyStatus.NO_KEY_PROVIDED:
            return self._encrypted  # True (confirmed encrypted) or None (couldn't tell)
        return True  # VERIFIED or INVALID

    def require_verified(self) -> None:
        """Raise before any catalog I/O if this repository is *confirmed*
        encrypted and its key hasn't been verified yet — gating this at
        the SDK level is what lets every consumer (CLI, TUI, a smoke-test
        tool, ...) get it for free, without an equivalent client-side
        check of its own.

        Deliberately narrower than ``status is KeyStatus.NO_KEY_PROVIDED``
        alone: that state also covers the rare case ``is_encrypted`` itself
        couldn't resolve to ``None``. Blocking on a genuinely unknown
        encryption status would be presumptuous — this only blocks once
        ``is_encrypted`` is confidently ``True``.

        Never called by ``Repository.catalogs()`` before it lists what's
        found: those rows are plaintext ``connection_config`` data needing
        no key, so the key prompt only appears once the user actually
        opens a catalog and its ``workloads()``/``versions()`` call this
        instead."""
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
        this manager's own state; ``Repository.set_key()`` decides what
        happens with the result (``adopt()``/``record()`` below), since
        that decision also depends on whether every already-open catalog
        successfully reopens under the new key, which this method knows
        nothing about.

        Deliberately cheap — no Pool scan; see ``dedup.keys`` for why the
        GCM-unwrap layer alone is sufficient."""
        keys = KeyMaterial.from_key_string(key_string)
        verification = await keys.verify(store, key_probe_layout(layout))
        return keys, verification

    def adopt(self, keys: KeyMaterial) -> None:
        """Commits ``keys`` as this manager's own key material immediately
        — called by ``Repository._reopen_catalogs_under_new_key`` right
        before it starts reopening each already-open catalog, so
        ``Repository._open_catalog_resources`` picks up the new key for those
        reopens (and for any not-yet-opened sibling's own eventual first
        open) rather than the stale one. Deliberately leaves
        ``status``/``verification`` untouched — ``record()`` updates those
        once the reopen sweep, whose own per-catalog success/failure has
        no bearing on whether the key itself was correct, has run to
        completion."""
        self._keys = keys

    def record(self, keys: KeyMaterial, verification: KeyVerification) -> None:
        """Records ``verification``'s outcome and recomputes ``status`` —
        called by ``Repository.set_key()`` once its own reopen sweep (if
        ``verification.ok`` triggered one) has finished, regardless of
        whether every catalog reopened cleanly: ``status`` must still
        distinguish "never tried" (``NO_KEY_PROVIDED``) from "tried and
        failed" (``INVALID``) even when a *rejected* key never reached
        ``adopt()`` at all, so ``keys`` — the just-tried key material, not
        necessarily ``self.keys`` — is taken as an explicit parameter
        rather than read back off this instance."""
        self._key_verification = verification
        self._status = _resolve_key_status(keys, verification, self._encrypted)
