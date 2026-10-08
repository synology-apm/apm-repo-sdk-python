"""Real values belonging to the recorded samples behind ``tests/fixtures/``.

Each is a generated artifact of one sample (its vault key, a wrapped key
record, a stable catalog identifier), not customer data, so it is safe to
commit. Each ``#:`` line names its sample by its ``manifest.TARGETS`` alias
(``targets.toml.example``). Replay tests under ``tests/integration/`` and
the anonymizer import them from here; ``tests/unit/`` uses synthetic values
only.
"""

from __future__ import annotations

#: ``vault-encrypted``'s generated vault key.
VAULT_ENCRYPTED_KEY_STRING = "n0wohSZahiKc@fHKnM74RWUBQnfgv4DWhXGmmEzV3GGwFpiHt99pjPeM="
#: ``vault-encrypted``'s wrapped VaultKey record
#: (``@ActiveProtectKey/userKey/<user_key_id>``).
VAULT_ENCRYPTED_WRAPPED_B64 = "99GzNFTm0omh+fXS14OwVpP2TwgcUwaM7HrttM8bAoW5jGPr7uI3rBa2//SxLss2"

#: ``objstore-encrypted``'s generated vault key.
OBJSTORE_ENCRYPTED_KEY_STRING = "IvcvldpbSRyd@Ys+mbaQyOElj6bHtL0+VFdp1e4swyEeApQLhdHgaTvg="
#: ``objstore-encrypted``'s wrapped VaultKey record.
OBJSTORE_ENCRYPTED_WRAPPED_B64 = "fESeS8zV25718Uz8AxuEJd5fgP8L00Pi77Em6mWGU6EgME8isn1ltaLsGhc5yC74"

#: ``pcps-encrypted``'s generated vault key.
PCPS_ENCRYPTED_KEY_STRING = "8ykvkIleOSKt@7KPuXLUuNT+bzm8r7CCe9+v8HiA0ztkZQ1fFgMjFc2w="

#: ``objstore-m365-encrypted``'s generated vault key.
OBJSTORE_M365_ENCRYPTED_KEY_STRING = "wLeLZp9tnAYw@s9m9JIplgBRHN4IPJ+75W8ttZ5okHyFjswEYwGc1K+o="

#: ``vault-plain``'s VM backup version (workload 2 of catalog 1).
VAULT_PLAIN_VM_VERSION_UID = "06b4b5e3-5490-4b6a-bee8-ce4287f7a9a7"
#: Canonical ref to ``VAULT_PLAIN_VM_VERSION_UID``.
VAULT_PLAIN_VM_REF = f"#cat:1/wl:2/ver:{VAULT_PLAIN_VM_VERSION_UID}"

#: ``vault-plain``'s FS backup version (FS workload 1).
VAULT_PLAIN_FS_VERSION_UID = "f72e8124-7e8f-43f7-afe5-52726835b3f3"
