"""Regression test for ``storage.generations`` — replayed from committed
fixtures recorded against real object-store samples, with **no external
dependency**: this always runs, on CI or anywhere else, because it goes
through ``ReplayStore`` instead of a real ``LocalFsStore``.

The fixtures (``tests/fixtures/``, recorded once by ``RecordingStore``):

- ``storage_generations_s3sample2_transactions.json.gz`` — rooted
  at ``s3-sample-2-encrypted``, every ``listdir()``/``exists()``/``read()``
  call ``latest_transaction_id``/``resolve_generation`` make resolving
  the real ``BikXpRbFNGI1`` repository's
  ``file_map``/``connection_config`` generations — no key material
  involved, since generation resolution never touches encrypted content.
  The real-key-dependent scenario for this same sample (opening the repository
  end to end and querying ``db("file_map")``) is already 100% covered by
  ``tests/integration/sdk/test_dedup_repository.py``'s own
  ``test_replayed_object_store_repos_open_and_expose_working_db_generation_selection``
  (it iterates over both real repo ids of that same sample,
  ``BikXpRbFNGI1`` included).
- ``storage_generations_sample1_repo_info.json.gz`` — rooted at
  ``sample-1``, opening the real ``5fkUi8kPsAlP`` repository (generation-
  suffixed ``repo_info.321``, unlike ``s3-sample-2-encrypted``'s bare
  ``repo_info``) with its real key — embedded below as a literal
  constant (this sample's own generated vault key, not customer data)
  rather than read from a real sample tree at test time.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from synology_apm_repo.sdk.dedup.keys import KeyMaterial
from synology_apm_repo.sdk.dedup.repository import DedupRepo
from synology_apm_repo.sdk.storage.base import ObjectStore
from synology_apm_repo.sdk.storage.generations import latest_transaction_id, resolve_generation
from synology_apm_repo.sdk.storage.layout import iter_layouts

_DB_DIR = "@ActiveProtectData/BikXpRbFNGI1/db"
_TXN_DIR = "@ActiveProtectData/BikXpRbFNGI1/repo_transactions"
_SUPPL_DIR = "@ActiveProtectData/BikXpRbFNGI1/suppl_transaction_ids"

#: sample-1's real key.
_SAMPLE1_KEY_STRING = "wLeLZp9tnAYw@s9m9JIplgBRHN4IPJ+75W8ttZ5okHyFjswEYwGc1K+o="


async def test_replayed_latest_txn_and_file_map_generation_match_real_s3_sample(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_generations_s3sample2_transactions.json.gz")

    # Confirmed by hand: repo_transaction.98 (the largest filename) embeds
    # transaction_id=100 — filename and embedded id genuinely differ.
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


async def test_replayed_repo_info_generation_selection_on_real_sample_1(
    record_target: Callable[[str], Awaitable[ObjectStore]],
) -> None:
    store = await record_target("storage_generations_sample1_repo_info.json.gz")
    matching = [layout async for layout in iter_layouts(store) if "5fkUi8kPsAlP" in layout.repo_root]
    assert matching, "expected the 5fkUi8kPsAlP repo id under this fixture"
    layout = matching[0]

    keys = KeyMaterial.from_key_string(_SAMPLE1_KEY_STRING)
    async with await DedupRepo.open(store, layout, keys) as repo:
        # Same real 5fkUi8kPsAlP repository as test_storage_s3.py's own
        # test_replayed_repo_info_reads_parse_to_the_real_uuids.
        assert repo.info.uuid == "2cO3L0Dv1TmjzLf9"


__all__: list[str] = []
