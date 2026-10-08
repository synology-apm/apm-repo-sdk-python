"""Regression test for ``--object-db-id`` at the CLI: a real Teams chat
version lists cleanly with no override, and a well-formed ``object_db_id``
naming a range that holds no ObjectDB still forces the raw view, which
fails with an ``error:``.

Fixture: ``object_db_id_cli_teams_chat_vault_plain.json.gz``, recorded against
``vault-plain/@ActiveProtectVault``.
``tests/integration/sdk/test_units_saas_raw_object_manual_object_db_id.py`` covers the same real data
through ``RawObjectProvider.create()``.
"""

from __future__ import annotations

from collections.abc import Callable

from support.cli import invoke

# Stable catalog ids of the real chat version and its stream.
_CCID = 3
_WORKLOAD_ID = 16
_VERSION_UID = "882d6f32-6cab-44e1-9b5c-cbb9d1bddcd3"
_STREAM_UUID = "uvWRSFkGxCcZAMwt"


def test_object_db_id_asks_for_the_raw_view_even_where_dispatch_succeeds_replayed(
    patch_profile_store: Callable[..., None],
) -> None:
    patch_profile_store("object_db_id_cli_teams_chat_vault_plain.json.gz", allow_content=True)

    ref = f"#cat:{_CCID}/wl:{_WORKLOAD_ID}/ver:{_VERSION_UID}"
    bogus_object_db_id = f"{_STREAM_UUID}_999999999_1234"

    invoke(["--json", "ls", ref, "--profile", "anything"])

    pinned_result = invoke(
        ["--json", "ls", ref, "--profile", "anything", "--object-db-id", bogus_object_db_id], exit_code=1
    )
    assert "error:" in pinned_result.stderr
