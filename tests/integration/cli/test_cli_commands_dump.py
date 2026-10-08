"""Regression tests for ``synology-apm-repo-cli dump bucket|composition|chunkmap``.

Two fixtures, both recorded against ``vault-plain``:

- ``cli_dump_bucket_vault_plain.json.gz`` — the bucket's header and SizeStore
  only, never its chunk data.
- ``cli_dump_composition_vault_plain.json.gz`` — one real composition file.
"""

from __future__ import annotations

import itertools
import json
from collections.abc import Callable

import synology_apm_repo.cli.commands.dump as dump_module
from support.cli import invoke

_BUCKET_REL = "@ActiveProtectVault/@data/Pool/61/4.buk.4"
_COMPOSITION_REL = "@ActiveProtectVault/@data/Composition/61/0.com/c0.12"


def test_dump_bucket_size_check_matches_real_file_replayed(patch_profile_store: Callable[..., None]) -> None:
    patch_profile_store("cli_dump_bucket_vault_plain.json.gz", dump_module)
    result = invoke(["--json", "dump", "bucket", "--profile", "demo", _BUCKET_REL])
    report = json.loads(result.stdout)
    assert report["major"] == 3
    assert report["chunk_num"] == 8192
    assert report["compress_type_counts"] == {"NONE": 8163, "ZSTD": 29}
    assert report["expected_size"] == 33598880
    assert report["actual_size"] == 33598880
    assert report["size_check_ok"] is True


def test_dump_composition_walks_all_four_real_records_exactly_to_eof_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_dump_composition_vault_plain.json.gz", dump_module)
    result = invoke(["--json", "dump", "composition", "--profile", "demo", _COMPOSITION_REL, "--limit", "10"])
    report = json.loads(result.stdout)
    assert report["header"] == {"major": 1, "minor": 1}
    head_offs = [r["head_off"] for r in report["records"]]
    map_nums = [r["map_num"] for r in report["records"]]
    assert head_offs == [64, 34572, 69080, 69991]
    assert map_nums == [896, 896, 20, 20]


def test_dump_composition_verify_map_confirms_every_real_record_crc_ok_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_dump_composition_vault_plain.json.gz", dump_module)
    result = invoke(["--json", "dump", "composition", "--profile", "demo", _COMPOSITION_REL, "--verify-map"])
    report = json.loads(result.stdout)
    assert len(report["records"]) == 4
    assert all(r["map_crc_ok"] for r in report["records"])


def test_dump_chunkmap_decodes_the_real_small_record_exactly_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("cli_dump_composition_vault_plain.json.gz", dump_module)
    result = invoke(
        ["--json", "dump", "chunkmap", "--profile", "demo", _COMPOSITION_REL, "--offset", "69080", "--verify-map"]
    )
    report = json.loads(result.stdout)
    assert report["map_num"] == 20
    assert report["map_crc_ok"] is True
    entries = report["entries"]
    assert len(entries) == 20
    assert entries[0]["addr"] == {"stream_id": 132, "bucket_id": 0, "chunk_idx": 0}
    assert entries[0]["file_offset"] == 0
    assert entries[9]["addr"] == {"stream_id": 61, "bucket_id": 102, "chunk_idx": 0}
    assert entries[9]["map_num"] == 1
    for prev, cur in itertools.pairwise(entries):
        assert prev["end_offset"] == cur["file_offset"]
