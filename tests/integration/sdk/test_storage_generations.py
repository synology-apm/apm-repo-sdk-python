"""Regression tests for ``storage.generations`` against real object-store
samples. Each fixture's one test is its recording recipe.

- ``storage_generations_objstore_encrypted_transactions.json.gz`` — recorded against
  ``objstore-encrypted``: ``latest_transaction_id``/
  ``resolve_generation`` resolving the ``BikXpRbFNGI1`` repository's
  ``file_map``/``connection_config`` generations. No key needed;
  opening that repository end to end with its key is covered by
  ``test_dedup_repository.py``'s
  ``test_replayed_object_store_repos_open_and_expose_working_db_generation_selection``.
- ``storage_generations_objstore_m365_encrypted_repo_info.json.gz`` — recorded against
  ``objstore-m365-encrypted``: opening the ``5fkUi8kPsAlP`` repository, whose
  ``repo_info`` is generation-suffixed (``repo_info.321``), unlike
  ``objstore-encrypted``'s bare ``repo_info``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from support.recording.sample_constants import OBJSTORE_M365_ENCRYPTED_KEY_STRING
from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.generations import latest_transaction_id, resolve_generation
from synology_apm_repo.sdk.storage.layout import catalog_repo_layouts, iter_repository_layouts

_DB_DIR = "@ActiveProtectData/BikXpRbFNGI1/db"
_TXN_DIR = "@ActiveProtectData/BikXpRbFNGI1/repo_transactions"
_SUPPL_DIR = "@ActiveProtectData/BikXpRbFNGI1/suppl_transaction_ids"


async def test_replayed_latest_txn_and_file_map_generation_match_real_objstore_encrypted(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_generations_objstore_encrypted_transactions.json.gz")

    # repo_transaction.98 (the largest filename) embeds transaction_id=100:
    # the filename and the embedded id differ.
    assert await latest_transaction_id(store, _TXN_DIR) == 100

    # file_map's real generations are 73,75,78,81,84,86,89,91,93,96,98 —
    # the largest one strictly less than 100 is 98 itself.
    path = await resolve_generation(store, _DB_DIR, "file_map", transactions_dir=_TXN_DIR, suppl_dir=_SUPPL_DIR)
    assert path == f"{_DB_DIR}/file_map.98"

    # connection_config is a supplemental table with markers 0..10 — must
    # resolve independently of file_map's own, unrelated numbering.
    path2 = await resolve_generation(
        store, _DB_DIR, "connection_config", transactions_dir=_TXN_DIR, suppl_dir=_SUPPL_DIR
    )
    assert path2 == f"{_DB_DIR}/connection_config.10"


async def test_replayed_repo_info_generation_selection_on_real_objstore_m365_encrypted(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_generations_objstore_m365_encrypted_repo_info.json.gz")
    matching = [
        layout
        async for repo in iter_repository_layouts(store)
        for layout in catalog_repo_layouts(repo)
        if "5fkUi8kPsAlP" in layout.repo_root
    ]
    assert matching, "expected the 5fkUi8kPsAlP repo id under this fixture"
    layout = matching[0]

    keys = KeyMaterial.from_key_string(OBJSTORE_M365_ENCRYPTED_KEY_STRING)
    async with await DedupRepo.open(store, layout, keys) as repo:
        assert repo.info.uuid == "2cO3L0Dv1TmjzLf9"
