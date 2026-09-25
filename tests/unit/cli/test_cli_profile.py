"""Unit tests for ``synology-apm-repo-cli profile add|list|show|remove``
(``synology_apm_repo.cli.commands.profile``).

Every profile-persistence function this command module imports
(``list_profiles_full``/``get_profile``/``save_profile``/``delete_profile``)
is monkeypatched to a thin wrapper binding ``config_dir=tmp_path`` — the CLI
itself exposes no ``--config-dir`` flag (only the SDK facade takes one, for
tests), so this is the CLI-side equivalent of what
``tests/unit/sdk/test_profiles_facade.py`` does directly against the SDK.
Secrets round-trip through the real, in-memory ``fake_keyring`` fixture
(``tests/conftest.py``) — no real OS keyring or network access.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

import synology_apm_repo.cli.commands.profile as profile_cmd
import synology_apm_repo.sdk.profiles as sdk_profiles
from synology_apm_repo.cli.main import app
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.profiles import BackendKind
from synology_apm_repo.sdk.storage.azure import AzureStore
from synology_apm_repo.sdk.storage.s3 import S3Store
from synology_apm_repo.sdk.storage.smb import SmbStore

runner = CliRunner()


async def _fake_store_from_fields(kind: object, fields: object) -> object:
    return object()


@pytest.fixture(autouse=True)
def _config_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def _list_profiles_full() -> list[sdk_profiles.Profile]:
        return await sdk_profiles.list_profiles_full(config_dir=tmp_path)

    async def _get_profile(name: str) -> sdk_profiles.Profile:
        return await sdk_profiles.get_profile(name, config_dir=tmp_path)

    async def _save_profile(name: str, kind: BackendKind, fields: dict[str, str | bool]) -> None:
        await sdk_profiles.save_profile(name, kind, fields, config_dir=tmp_path)

    async def _delete_profile(name: str) -> None:
        await sdk_profiles.delete_profile(name, config_dir=tmp_path)

    monkeypatch.setattr(profile_cmd, "list_profiles_full", _list_profiles_full)
    monkeypatch.setattr(profile_cmd, "get_profile", _get_profile)
    monkeypatch.setattr(profile_cmd, "save_profile", _save_profile)
    monkeypatch.setattr(profile_cmd, "delete_profile", _delete_profile)


# -- add --------------------------------------------------------------------


def test_add_no_verify_persists_without_secrets(fake_keyring: None) -> None:
    result = runner.invoke(
        app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--no-verify"], input="\n\n"
    )
    assert result.exit_code == 0, result.output
    assert "saved" in result.output

    show = runner.invoke(app, ["--json", "profile", "show", "demo"])
    report = json.loads(show.stdout)
    assert report == {
        "name": "demo",
        "kind": "s3",
        "verify_tls": True,
        "bucket": "my-bucket",
        "endpoint": None,
        "region": None,
    }


def test_add_azure_no_verify_persists(fake_keyring: None) -> None:
    result = runner.invoke(
        app, ["profile", "add", "demo", "--backend", "azure", "--container", "my-container", "--no-verify"], input="\n"
    )
    assert result.exit_code == 0, result.output

    show = runner.invoke(app, ["--json", "profile", "show", "demo"])
    report = json.loads(show.stdout)
    assert report["container"] == "my-container"
    assert report["kind"] == "azure"


def test_add_smb_no_verify_persists(fake_keyring: None) -> None:
    result = runner.invoke(
        app,
        [
            "profile",
            "add",
            "demo",
            "--backend",
            "smb",
            "--server",
            "nas.example.com",
            "--share",
            "backups",
            "--no-verify",
        ],
        input="\n",
    )
    assert result.exit_code == 0, result.output

    show = runner.invoke(app, ["--json", "profile", "show", "demo"])
    report = json.loads(show.stdout)
    assert report == {
        "name": "demo",
        "kind": "smb",
        "server": "nas.example.com",
        "share": "backups",
        "port": 445,
        "username": None,
    }


def test_add_s3_requires_bucket() -> None:
    result = runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--no-verify"])
    assert result.exit_code == 1
    assert "--bucket is required" in result.output


def test_add_azure_requires_container() -> None:
    result = runner.invoke(app, ["profile", "add", "demo", "--backend", "azure", "--no-verify"])
    assert result.exit_code == 1
    assert "--container is required" in result.output


def test_add_smb_requires_server_and_share() -> None:
    result = runner.invoke(app, ["profile", "add", "demo", "--backend", "smb", "--no-verify"])
    assert result.exit_code == 1
    assert "--server and --share are required" in result.output


def test_add_refuses_to_overwrite_without_force(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "one", "--no-verify"], input="\n\n")
    result = runner.invoke(
        app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "two", "--no-verify"], input="\n\n"
    )
    assert result.exit_code == 1
    assert "already exists" in result.output


def test_add_force_overwrites(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "one", "--no-verify"], input="\n\n")
    result = runner.invoke(
        app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "two", "--force", "--no-verify"], input="\n\n"
    )
    assert result.exit_code == 0, result.output

    show = runner.invoke(app, ["--json", "profile", "show", "demo"])
    assert json.loads(show.stdout)["bucket"] == "two"


def test_add_secrets_never_appear_in_config_json(fake_keyring: None, tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--no-verify"],
        input="AKIA-SECRET\nSHHH-SECRET\n",
    )
    assert result.exit_code == 0, result.output
    saved = (tmp_path / "profiles.json").read_text()
    assert "AKIA-SECRET" not in saved
    assert "SHHH-SECRET" not in saved

    show = runner.invoke(app, ["--json", "profile", "show", "demo"])
    assert "AKIA-SECRET" not in show.stdout
    assert "SHHH-SECRET" not in show.stdout


def test_add_verify_failure_does_not_persist(monkeypatch: pytest.MonkeyPatch, fake_keyring: None) -> None:
    class _FakeSession:
        async def open_remote(self, store: object, *args: object, **kwargs: object) -> list[object]:
            return []

        async def close(self) -> None:
            pass

    monkeypatch.setattr(profile_cmd, "Session", _FakeSession)
    monkeypatch.setattr(profile_cmd, "_store_from_fields", _fake_store_from_fields)

    result = runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket"], input="\n\n")
    assert result.exit_code == 1
    assert "no repositories" in result.output

    listing = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


def test_add_azure_unresolvable_account_url_fails_cleanly_instead_of_crashing(fake_keyring: None) -> None:
    """``AzureStore(...)``/``BlobServiceClient(...)`` construction
    validates its account_url/credential shape synchronously and can
    raise ``ValueError`` (an
    account URL with no path and no recognizable
    ``.blob.core.<...>`` subdomain — the account name is genuinely
    unresolvable here) — unlike ``S3Store``, whose constructor defers
    everything to first read/listdir. ``_store_from_fields()`` isn't
    monkeypatched here, so this exercises the real ``AzureStore(...)``
    call and the ``add()`` command's own exception handling around it,
    rather than crashing with a raw traceback."""
    result = runner.invoke(
        app,
        [
            "profile",
            "add",
            "demo",
            "--backend",
            "azure",
            "--container",
            "my-container",
            "--account-url",
            "http://192.0.2.10:10000",
        ],
        input="some-key\n",
    )
    assert result.exit_code == 1
    assert "connectivity check failed" in result.output
    assert "Traceback" not in result.output

    listing = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


def test_add_verify_success_persists(monkeypatch: pytest.MonkeyPatch, fake_keyring: None) -> None:
    class _FakeSession:
        async def open_remote(self, store: object, *args: object, **kwargs: object) -> list[object]:
            return [object()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr(profile_cmd, "Session", _FakeSession)
    monkeypatch.setattr(profile_cmd, "_store_from_fields", _fake_store_from_fields)

    result = runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket"], input="\n\n")
    assert result.exit_code == 0, result.output
    assert "verified" in result.output

    listing = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(listing.stdout) == [{"name": "demo", "kind": "s3"}]


def test_add_s3_verify_uses_the_real_store_from_fields_with_endpoint_and_region(
    monkeypatch: pytest.MonkeyPatch, fake_keyring: None
) -> None:
    """Unlike every other ``--verify``-exercising test above,
    ``_store_from_fields`` is deliberately left un-patched here — this is
    the one test that runs the real S3 branch of it (``--endpoint``/
    ``--region`` included, so both optional-field guards run too)."""

    class _FakeSession:
        async def open_remote(self, store: object, *args: object, **kwargs: object) -> list[object]:
            assert isinstance(store, S3Store)  # proves the real _store_from_fields built this
            return [object()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr(profile_cmd, "Session", _FakeSession)

    result = runner.invoke(
        app,
        [
            "profile",
            "add",
            "demo",
            "--backend",
            "s3",
            "--bucket",
            "my-bucket",
            "--endpoint",
            "http://minio:9000",
            "--region",
            "us-east-1",
        ],
        input="\n\n",
    )
    assert result.exit_code == 0, result.output


def test_add_azure_verify_uses_the_real_store_from_fields(monkeypatch: pytest.MonkeyPatch, fake_keyring: None) -> None:
    """Azure's own counterpart to
    ``test_add_s3_verify_uses_the_real_store_from_fields_with_endpoint_and_region``
    -- every other azure test either passes ``--no-verify`` or a
    deliberately unresolvable ``--account-url`` to prove the
    *failure* path; this is the only one exercising the real
    ``_store_from_fields`` azure branch all the way to a
    successful verify."""

    class _FakeSession:
        async def open_remote(self, store: object, *args: object, **kwargs: object) -> list[object]:
            assert isinstance(store, AzureStore)  # proves the real _store_from_fields built this
            return [object()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr(profile_cmd, "Session", _FakeSession)

    result = runner.invoke(
        app,
        [
            "profile",
            "add",
            "demo",
            "--backend",
            "azure",
            "--container",
            "my-container",
            "--account-url",
            "https://myaccount.blob.core.windows.net",
        ],
        input="some-key\n",
    )
    assert result.exit_code == 0, result.output
    assert "verified" in result.output


def test_add_smb_verify_uses_the_real_store_from_fields(monkeypatch: pytest.MonkeyPatch, fake_keyring: None) -> None:
    """SMB's own counterpart to
    ``test_add_s3_verify_uses_the_real_store_from_fields_with_endpoint_and_region``/
    ``test_add_azure_verify_uses_the_real_store_from_fields``."""

    class _FakeSession:
        async def open_remote(self, store: object, *args: object, **kwargs: object) -> list[object]:
            assert isinstance(store, SmbStore)  # proves the real _store_from_fields built this
            return [object()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr(profile_cmd, "Session", _FakeSession)

    result = runner.invoke(
        app,
        [
            "profile",
            "add",
            "demo",
            "--backend",
            "smb",
            "--server",
            "nas.example.com",
            "--share",
            "backups",
            "--username",
            "admin",
        ],
        input="hunter2\n",
    )
    assert result.exit_code == 0, result.output
    assert "verified" in result.output


def test_add_quiet_suppresses_the_verified_and_saved_lines(monkeypatch: pytest.MonkeyPatch, fake_keyring: None) -> None:
    class _FakeSession:
        async def open_remote(self, store: object, *args: object, **kwargs: object) -> list[object]:
            return [object()]

        async def close(self) -> None:
            pass

    monkeypatch.setattr(profile_cmd, "Session", _FakeSession)
    monkeypatch.setattr(profile_cmd, "_store_from_fields", _fake_store_from_fields)

    result = runner.invoke(
        app, ["--quiet", "profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket"], input="\n\n"
    )
    assert result.exit_code == 0, result.output
    assert "verified" not in result.output
    assert "saved" not in result.output

    # --quiet never hides the profile actually existing afterward.
    listing = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(listing.stdout) == [{"name": "demo", "kind": "s3"}]


def test_add_no_input_reads_secrets_from_stdin_in_prompt_order(fake_keyring: None, tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["--no-input", "profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--no-verify"],
        input="AKIA-SECRET\nSHHH-SECRET\n",
    )
    assert result.exit_code == 0, result.output
    # No interactive prompt text leaks into the output under --no-input.
    assert "Access key" not in result.output
    saved = (tmp_path / "profiles.json").read_text()
    assert "AKIA-SECRET" not in saved
    assert "SHHH-SECRET" not in saved


def test_add_no_input_blank_lines_mean_ambient_credential_chain(fake_keyring: None) -> None:
    result = runner.invoke(
        app,
        ["--no-input", "profile", "add", "demo", "--backend", "azure", "--container", "c", "--no-verify"],
        input="\n",
    )
    assert result.exit_code == 0, result.output


def test_add_connectivity_check_raising_apm_repo_error_fails_cleanly(
    monkeypatch: pytest.MonkeyPatch, fake_keyring: None
) -> None:
    class _FakeSession:
        async def open_remote(self, store: object, *args: object, **kwargs: object) -> list[object]:
            raise ApmRepoError("connection refused")

        async def close(self) -> None:
            pass

    monkeypatch.setattr(profile_cmd, "Session", _FakeSession)
    monkeypatch.setattr(profile_cmd, "_store_from_fields", _fake_store_from_fields)

    result = runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket"], input="\n\n")
    assert result.exit_code == 1
    assert "connectivity check failed" in result.output
    assert "connection refused" in result.output

    listing = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


# -- list ---------------------------------------------------------------


def test_list_empty_human() -> None:
    result = runner.invoke(app, ["profile", "list"])
    assert result.exit_code == 0, result.output
    assert "no saved profiles" in result.output


def test_list_shows_saved_profiles(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = runner.invoke(app, ["profile", "list"])
    assert result.exit_code == 0, result.output
    assert "demo" in result.output
    assert "s3" in result.output


def test_list_verbose_empty_human() -> None:
    result = runner.invoke(app, ["--verbose", "profile", "list"])
    assert result.exit_code == 0, result.output
    assert "no saved profiles" in result.output


def test_list_verbose_human_separates_multiple_profiles_with_a_blank_line(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "one", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    runner.invoke(app, ["profile", "add", "two", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = runner.invoke(app, ["--verbose", "profile", "list"])
    assert result.exit_code == 0, result.output
    assert "name       : one" in result.output
    assert "name       : two" in result.output
    assert "\n\nname       : two" in result.output


def test_list_verbose_human_shows_the_same_fields_show_does(fake_keyring: None) -> None:
    runner.invoke(
        app,
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--region", "us-east-1", "--no-verify"],
        input="\n\n",
    )
    show = runner.invoke(app, ["profile", "show", "demo"])
    result = runner.invoke(app, ["--verbose", "profile", "list"])
    assert result.exit_code == 0, result.output
    for line in ("backend    : s3", "bucket     : my-bucket", "region     : us-east-1", "verify_tls : True"):
        assert line in show.output
        assert line in result.output


def test_list_verbose_json_shows_the_same_fields_show_does(fake_keyring: None) -> None:
    runner.invoke(
        app, ["profile", "add", "demo", "--backend", "azure", "--container", "my-container", "--no-verify"], input="\n"
    )
    show = runner.invoke(app, ["--json", "profile", "show", "demo"])
    listing = runner.invoke(app, ["--json", "--verbose", "profile", "list"])
    assert listing.exit_code == 0, listing.output
    assert json.loads(listing.stdout) == [json.loads(show.stdout)]


def test_list_verbose_covers_every_backend_kind(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "s3-demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    runner.invoke(
        app, ["profile", "add", "azure-demo", "--backend", "azure", "--container", "c", "--no-verify"], input="\n"
    )
    runner.invoke(
        app,
        ["profile", "add", "smb-demo", "--backend", "smb", "--server", "s", "--share", "sh", "--no-verify"],
        input="\n",
    )
    result = runner.invoke(app, ["--json", "--verbose", "profile", "list"])
    assert result.exit_code == 0, result.output
    reports = {r["name"]: r for r in json.loads(result.stdout)}
    assert reports["s3-demo"]["bucket"] == "b"
    assert reports["azure-demo"]["container"] == "c"
    assert reports["smb-demo"]["server"] == "s"


def test_list_non_verbose_output_is_unchanged_by_the_verbose_addition(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(result.stdout) == [{"name": "demo", "kind": "s3"}]


# -- show -----------------------------------------------------------------


def test_show_missing_profile_fails() -> None:
    result = runner.invoke(app, ["profile", "show", "nope"])
    assert result.exit_code == 1
    assert "no such profile" in result.output


def test_show_human_output_s3(fake_keyring: None) -> None:
    runner.invoke(
        app,
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--region", "us-east-1", "--no-verify"],
        input="\n\n",
    )
    result = runner.invoke(app, ["profile", "show", "demo"])
    assert result.exit_code == 0, result.output
    assert "backend    : s3" in result.output
    assert "bucket     : my-bucket" in result.output
    assert "region     : us-east-1" in result.output
    assert "endpoint   : (default)" in result.output
    assert "verify_tls : True" in result.output


def test_add_no_verify_tls_persists_false_s3(fake_keyring: None) -> None:
    result = runner.invoke(
        app,
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--no-verify", "--no-verify-tls"],
        input="\n\n",
    )
    assert result.exit_code == 0, result.output

    show = runner.invoke(app, ["--json", "profile", "show", "demo"])
    report = json.loads(show.stdout)
    assert report["verify_tls"] is False


def test_add_no_verify_tls_persists_false_azure(fake_keyring: None) -> None:
    result = runner.invoke(
        app,
        [
            "profile",
            "add",
            "demo",
            "--backend",
            "azure",
            "--container",
            "my-container",
            "--no-verify",
            "--no-verify-tls",
        ],
        input="\n",
    )
    assert result.exit_code == 0, result.output

    show = runner.invoke(app, ["--json", "profile", "show", "demo"])
    report = json.loads(show.stdout)
    assert report["verify_tls"] is False


def test_add_no_verify_tls_is_silently_ignored_for_smb(fake_keyring: None) -> None:
    """SMB has no TLS concept (its own ``SmbProfileConfig`` carries no
    ``verify_tls`` field at all) -- ``--no-verify-tls`` with ``--backend
    smb`` is accepted, not rejected, matching how every other
    backend-mismatched flag (e.g. ``--region`` with SMB) already behaves."""
    result = runner.invoke(
        app,
        [
            "profile",
            "add",
            "demo",
            "--backend",
            "smb",
            "--server",
            "nas.example.com",
            "--share",
            "backups",
            "--no-verify",
            "--no-verify-tls",
        ],
        input="\n",
    )
    assert result.exit_code == 0, result.output

    show = runner.invoke(app, ["--json", "profile", "show", "demo"])
    report = json.loads(show.stdout)
    assert "verify_tls" not in report


def test_show_human_output_s3_with_verify_tls_disabled(fake_keyring: None) -> None:
    runner.invoke(
        app,
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--no-verify", "--no-verify-tls"],
        input="\n\n",
    )
    result = runner.invoke(app, ["profile", "show", "demo"])
    assert result.exit_code == 0, result.output
    assert "verify_tls : False" in result.output


def test_show_human_output_azure(fake_keyring: None) -> None:
    runner.invoke(
        app, ["profile", "add", "demo", "--backend", "azure", "--container", "my-container", "--no-verify"], input="\n"
    )
    result = runner.invoke(app, ["profile", "show", "demo"])
    assert result.exit_code == 0, result.output
    assert "backend    : azure" in result.output
    assert "container  : my-container" in result.output
    assert "account_url: (default)" in result.output


def test_show_human_output_smb(fake_keyring: None) -> None:
    runner.invoke(
        app,
        [
            "profile",
            "add",
            "demo",
            "--backend",
            "smb",
            "--server",
            "nas.example.com",
            "--share",
            "backups",
            "--username",
            "admin",
            "--no-verify",
        ],
        input="hunter2\n",
    )
    result = runner.invoke(app, ["profile", "show", "demo"])
    assert result.exit_code == 0, result.output
    assert "backend    : smb" in result.output
    assert "server     : nas.example.com" in result.output
    assert "share      : backups" in result.output
    assert "port       : 445" in result.output
    assert "username   : admin" in result.output
    assert "verify_tls" not in result.output  # SMB has no TLS concept — never rendered


# -- remove -----------------------------------------------------------------


def test_remove_with_force(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = runner.invoke(app, ["profile", "remove", "demo", "--force"])
    assert result.exit_code == 0, result.output
    assert "removed" in result.output

    listing = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


def test_remove_without_force_prompts_and_can_be_declined(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = runner.invoke(app, ["profile", "remove", "demo"], input="n\n")
    assert result.exit_code == 0, result.output
    assert "aborted" in result.output

    listing = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(listing.stdout) == [{"name": "demo", "kind": "s3"}]


def test_remove_without_force_confirmed(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = runner.invoke(app, ["profile", "remove", "demo"], input="y\n")
    assert result.exit_code == 0, result.output

    listing = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


def test_remove_missing_profile_fails() -> None:
    result = runner.invoke(app, ["profile", "remove", "nope", "--force"])
    assert result.exit_code == 1
    assert "no such profile" in result.output


def test_remove_quiet_suppresses_the_removed_line(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = runner.invoke(app, ["--quiet", "profile", "remove", "demo", "--force"])
    assert result.exit_code == 0, result.output
    assert "removed" not in result.output

    listing = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


def test_remove_no_input_without_force_fails_instead_of_prompting(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = runner.invoke(app, ["--no-input", "profile", "remove", "demo"])
    assert result.exit_code == 1
    assert "--no-input requires --force" in result.output

    # Never removed — the command must fail before touching anything.
    listing = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(listing.stdout) == [{"name": "demo", "kind": "s3"}]


def test_remove_no_input_with_force_skips_confirmation(fake_keyring: None) -> None:
    runner.invoke(app, ["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = runner.invoke(app, ["--no-input", "profile", "remove", "demo", "--force"])
    assert result.exit_code == 0, result.output

    listing = runner.invoke(app, ["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


# -- --no-input's stdin-is-a-terminal guard ----------------------------------


def test_require_piped_stdin_fails_fast_when_stdin_is_a_real_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    # CliRunner's own stdin is never a real tty, so the command-level tests
    # above only ever exercise the "read the piped line" branch — this is
    # the one place the isatty() fail-fast branch itself gets covered.
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    with pytest.raises(typer.Exit):
        profile_cmd._require_piped_stdin()


__all__: list[str] = []
