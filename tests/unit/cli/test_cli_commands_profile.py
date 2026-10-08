"""Unit tests for ``synology-apm-repo-cli profile add|list|show|remove``
(``synology_apm_repo.cli.commands.profile``).

The command's ``list_profiles``/``get_profile``/``save_profile``/
``delete_profile`` are rebound to ``config_dir=tmp_path`` (the CLI has no
``--config-dir`` flag); secrets go through ``tests/unit/conftest.py``'s
in-memory ``fake_keyring``.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest
import typer
from inline_snapshot import snapshot

import synology_apm_repo.cli.commands.profile as profile_cmd
import synology_apm_repo.sdk.profiles as sdk_profiles
from support.cli import invoke
from synology_apm_repo.sdk.errors import ApmRepoError
from synology_apm_repo.sdk.profiles import BackendKind
from synology_apm_repo.sdk.storage.azure import AzureStore
from synology_apm_repo.sdk.storage.s3 import S3Store
from synology_apm_repo.sdk.storage.smb import SmbStore
from unit.cli.session_fakes import install_fake_session


async def _fake_store_from_fields(kind: object, fields: object) -> object:
    return object()


@pytest.fixture(autouse=True)
def _config_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def _list_profiles() -> list[sdk_profiles.Profile]:
        return await sdk_profiles.list_profiles(config_dir=tmp_path)

    async def _get_profile(name: str) -> sdk_profiles.Profile:
        return await sdk_profiles.get_profile(name, config_dir=tmp_path)

    async def _save_profile(name: str, kind: BackendKind, fields: dict[str, str | bool]) -> None:
        await sdk_profiles.save_profile(name, kind, fields, config_dir=tmp_path)

    async def _delete_profile(name: str) -> None:
        await sdk_profiles.delete_profile(name, config_dir=tmp_path)

    monkeypatch.setattr(profile_cmd, "list_profiles", _list_profiles)
    monkeypatch.setattr(profile_cmd, "get_profile", _get_profile)
    monkeypatch.setattr(profile_cmd, "save_profile", _save_profile)
    monkeypatch.setattr(profile_cmd, "delete_profile", _delete_profile)


# -- add --------------------------------------------------------------------


def test_add_no_verify_persists_without_secrets(fake_keyring: None) -> None:
    result = invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--no-verify"], input="\n\n")
    assert result.output == snapshot("""\
Access key (blank for ambient credential chain): \n\
Secret key (blank for ambient credential chain): \n\
saved profile 'demo' (s3)
""")

    show = invoke(["--json", "profile", "show", "demo"])
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
    invoke(["profile", "add", "demo", "--backend", "azure", "--container", "my-container", "--no-verify"], input="\n")

    show = invoke(["--json", "profile", "show", "demo"])
    report = json.loads(show.stdout)
    assert report["container"] == "my-container"
    assert report["kind"] == "azure"


def test_add_smb_no_verify_persists(fake_keyring: None) -> None:
    invoke(
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

    show = invoke(["--json", "profile", "show", "demo"])
    report = json.loads(show.stdout)
    assert report == {
        "name": "demo",
        "kind": "smb",
        "server": "nas.example.com",
        "share": "backups",
        "port": 445,
        "username": None,
    }


@pytest.mark.parametrize(
    ("backend", "required"),
    [
        pytest.param("s3", "--bucket", id="s3_requires_bucket"),
        pytest.param("azure", "--container", id="azure_requires_container"),
    ],
)
def test_add_requires_the_backend_location_option(backend: str, required: str) -> None:
    result = invoke(["profile", "add", "demo", "--backend", backend, "--no-verify"], exit_code=1)
    assert result.output == f"error: {required} is required for --backend {backend}\n"


def test_add_smb_requires_server_and_share() -> None:
    result = invoke(["profile", "add", "demo", "--backend", "smb", "--no-verify"], exit_code=1)
    assert result.output == snapshot("error: --server is required for --backend smb\n")
    result = invoke(["profile", "add", "demo", "--backend", "smb", "--server", "nas", "--no-verify"], exit_code=1)
    assert result.output == snapshot("error: --share is required for --backend smb\n")


def test_add_refuses_to_overwrite_without_force(fake_keyring: None) -> None:
    invoke(
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "one", "--no-verify"], input="\n\n", exit_code=None
    )
    result = invoke(
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "two", "--no-verify"], input="\n\n", exit_code=1
    )
    assert result.output == snapshot("error: profile 'demo' already exists — use --force to overwrite\n")


def test_add_force_overwrites(fake_keyring: None) -> None:
    invoke(
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "one", "--no-verify"], input="\n\n", exit_code=None
    )
    invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "two", "--force", "--no-verify"], input="\n\n")

    show = invoke(["--json", "profile", "show", "demo"])
    assert json.loads(show.stdout)["bucket"] == "two"


def test_add_secrets_never_appear_in_config_json(fake_keyring: None, tmp_path: Path) -> None:
    invoke(
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--no-verify"],
        input="AKIA-SECRET\nSHHH-SECRET\n",
    )
    saved = (tmp_path / "profiles.json").read_text()
    assert "AKIA-SECRET" not in saved
    assert "SHHH-SECRET" not in saved

    show = invoke(["--json", "profile", "show", "demo"], exit_code=None)
    assert "AKIA-SECRET" not in show.stdout
    assert "SHHH-SECRET" not in show.stdout


def test_add_verify_failure_does_not_persist(monkeypatch: pytest.MonkeyPatch, fake_keyring: None) -> None:
    install_fake_session(monkeypatch, [])
    monkeypatch.setattr(profile_cmd, "store_from_fields", _fake_store_from_fields)

    result = invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket"], input="\n\n", exit_code=1)
    assert result.output == snapshot("""\
Access key (blank for ambient credential chain): \n\
Secret key (blank for ambient credential chain): \n\
error: connectivity check found no repositories — check the bucket/container and
credentials, or pass --no-verify to save anyway
""")

    listing = invoke(["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


def test_add_azure_unresolvable_account_url_fails_cleanly_instead_of_crashing(fake_keyring: None) -> None:
    """The real ``AzureStore(...)`` raises ``ValueError`` for an account URL
    with no path and no ``.blob.core.<...>`` subdomain."""
    result = invoke(
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
        exit_code=1,
    )
    assert "connectivity check failed" in result.output
    assert "Traceback" not in result.output

    listing = invoke(["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


def test_add_verify_success_persists(monkeypatch: pytest.MonkeyPatch, fake_keyring: None) -> None:
    install_fake_session(monkeypatch, [object()])
    monkeypatch.setattr(profile_cmd, "store_from_fields", _fake_store_from_fields)

    result = invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket"], input="\n\n")
    assert result.output == snapshot("""\
Access key (blank for ambient credential chain): \n\
Secret key (blank for ambient credential chain): \n\
verified — found 1 repository
saved profile 'demo' (s3)
""")

    listing = invoke(["--json", "profile", "list"])
    assert json.loads(listing.stdout) == [{"name": "demo", "kind": "s3"}]


def test_add_s3_verify_uses_the_real_store_from_fields_with_endpoint_and_region(
    monkeypatch: pytest.MonkeyPatch, fake_keyring: None
) -> None:
    session = install_fake_session(monkeypatch, [object()])

    invoke(
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
    assert [type(call["source"]) for call in session.open_calls] == [S3Store]


def test_add_azure_verify_uses_the_real_store_from_fields(monkeypatch: pytest.MonkeyPatch, fake_keyring: None) -> None:
    session = install_fake_session(monkeypatch, [object()])

    result = invoke(
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
    assert [type(call["source"]) for call in session.open_calls] == [AzureStore]
    assert result.output == snapshot("""\
Credential — account key or SAS token (blank for ambient credential chain): \n\
verified — found 1 repository
saved profile 'demo' (azure)
""")


def test_add_smb_verify_uses_the_real_store_from_fields(monkeypatch: pytest.MonkeyPatch, fake_keyring: None) -> None:
    session = install_fake_session(monkeypatch, [object()])

    result = invoke(
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
    assert [type(call["source"]) for call in session.open_calls] == [SmbStore]
    assert result.output == snapshot("""\
Password (blank for an anonymous/guest session): \n\
verified — found 1 repository
saved profile 'demo' (smb)
""")


def test_add_quiet_suppresses_the_verified_and_saved_lines(monkeypatch: pytest.MonkeyPatch, fake_keyring: None) -> None:
    install_fake_session(monkeypatch, [object()])
    monkeypatch.setattr(profile_cmd, "store_from_fields", _fake_store_from_fields)

    result = invoke(["--quiet", "profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket"], input="\n\n")
    assert result.output == snapshot("""\
Access key (blank for ambient credential chain): \n\
Secret key (blank for ambient credential chain): \n\
""")

    listing = invoke(["--json", "profile", "list"])
    assert json.loads(listing.stdout) == [{"name": "demo", "kind": "s3"}]


def test_add_no_input_reads_secrets_from_stdin_in_prompt_order(fake_keyring: None, tmp_path: Path) -> None:
    result = invoke(
        ["--no-input", "profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--no-verify"],
        input="AKIA-SECRET\nSHHH-SECRET\n",
    )
    assert result.output == snapshot("saved profile 'demo' (s3)\n")
    saved = (tmp_path / "profiles.json").read_text()
    assert "AKIA-SECRET" not in saved
    assert "SHHH-SECRET" not in saved
    fields = asyncio.run(sdk_profiles.profile_fields_with_secrets("demo", config_dir=tmp_path))
    assert (fields["access_key"], fields["secret_key"]) == ("AKIA-SECRET", "SHHH-SECRET")


def test_add_no_input_blank_lines_mean_ambient_credential_chain(fake_keyring: None, tmp_path: Path) -> None:
    result = invoke(
        ["--no-input", "profile", "add", "demo", "--backend", "azure", "--container", "c", "--no-verify"], input="\n"
    )
    assert result.output == snapshot("saved profile 'demo' (azure)\n")
    fields = asyncio.run(sdk_profiles.profile_fields_with_secrets("demo", config_dir=tmp_path))
    assert "credential" not in fields


def test_add_connectivity_check_raising_apm_repo_error_fails_cleanly(
    monkeypatch: pytest.MonkeyPatch, fake_keyring: None
) -> None:
    install_fake_session(monkeypatch, open_error=ApmRepoError("connection refused"))
    monkeypatch.setattr(profile_cmd, "store_from_fields", _fake_store_from_fields)

    result = invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket"], input="\n\n", exit_code=1)
    assert result.output == snapshot("""\
Access key (blank for ambient credential chain): \n\
Secret key (blank for ambient credential chain): \n\
error: connectivity check failed: connection refused
""")

    listing = invoke(["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


# -- list ---------------------------------------------------------------


def test_list_empty_human() -> None:
    result = invoke(["profile", "list"])
    assert result.output == snapshot("(no saved profiles)\n")


def test_list_shows_saved_profiles(fake_keyring: None) -> None:
    invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = invoke(["profile", "list"])
    assert result.output == snapshot("demo  s3\n")


def test_list_verbose_empty_human() -> None:
    result = invoke(["--verbose", "profile", "list"])
    assert result.output == snapshot("(no saved profiles)\n")


def test_list_verbose_human_separates_multiple_profiles_with_a_blank_line(fake_keyring: None) -> None:
    invoke(["profile", "add", "one", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    invoke(["profile", "add", "two", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = invoke(["--verbose", "profile", "list"])
    assert result.output == snapshot("""\
name       : one
backend    : s3
bucket     : b
endpoint   : (default)
region     : (default)
verify_tls : True

name       : two
backend    : s3
bucket     : b
endpoint   : (default)
region     : (default)
verify_tls : True
""")


def test_list_verbose_human_shows_the_same_fields_show_does(fake_keyring: None) -> None:
    invoke(
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--region", "us-east-1", "--no-verify"],
        input="\n\n",
        exit_code=None,
    )
    show = invoke(["profile", "show", "demo"])
    result = invoke(["--verbose", "profile", "list"])
    assert (
        result.output
        == show.output
        == snapshot("""\
name       : demo
backend    : s3
bucket     : my-bucket
endpoint   : (default)
region     : us-east-1
verify_tls : True
""")
    )


def test_list_verbose_json_shows_the_same_fields_show_does(fake_keyring: None) -> None:
    invoke(
        ["profile", "add", "demo", "--backend", "azure", "--container", "my-container", "--no-verify"],
        input="\n",
        exit_code=None,
    )
    show = invoke(["--json", "profile", "show", "demo"])
    listing = invoke(["--json", "--verbose", "profile", "list"])
    assert json.loads(listing.stdout) == [json.loads(show.stdout)]


def test_list_verbose_covers_every_backend_kind(fake_keyring: None) -> None:
    invoke(
        ["profile", "add", "s3-demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n", exit_code=None
    )
    invoke(
        ["profile", "add", "azure-demo", "--backend", "azure", "--container", "c", "--no-verify"],
        input="\n",
        exit_code=None,
    )
    invoke(
        ["profile", "add", "smb-demo", "--backend", "smb", "--server", "s", "--share", "sh", "--no-verify"],
        input="\n",
        exit_code=None,
    )
    result = invoke(["--json", "--verbose", "profile", "list"])
    reports = {r["name"]: r for r in json.loads(result.stdout)}
    assert reports["s3-demo"]["bucket"] == "b"
    assert reports["azure-demo"]["container"] == "c"
    assert reports["smb-demo"]["server"] == "s"


def test_list_non_verbose_output_is_unchanged_by_the_verbose_addition(fake_keyring: None) -> None:
    invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = invoke(["--json", "profile", "list"])
    assert json.loads(result.stdout) == [{"name": "demo", "kind": "s3"}]


# -- show -----------------------------------------------------------------


def test_show_missing_profile_fails() -> None:
    result = invoke(["profile", "show", "nope"], exit_code=1)
    assert result.output == snapshot("error: no such profile: 'nope'\n")


def test_show_human_output_s3(fake_keyring: None) -> None:
    invoke(
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--region", "us-east-1", "--no-verify"],
        input="\n\n",
        exit_code=None,
    )
    result = invoke(["profile", "show", "demo"])
    assert result.output == snapshot("""\
name       : demo
backend    : s3
bucket     : my-bucket
endpoint   : (default)
region     : us-east-1
verify_tls : True
""")


@pytest.mark.parametrize(
    ("backend_args", "prompt_input"),
    [
        pytest.param(["--backend", "s3", "--bucket", "my-bucket"], "\n\n", id="s3"),
        pytest.param(["--backend", "azure", "--container", "my-container"], "\n", id="azure"),
    ],
)
def test_add_no_verify_tls_persists_false(fake_keyring: None, backend_args: list[str], prompt_input: str) -> None:
    invoke(["profile", "add", "demo", *backend_args, "--no-verify", "--no-verify-tls"], input=prompt_input)

    show = invoke(["--json", "profile", "show", "demo"])
    report = json.loads(show.stdout)
    assert report["verify_tls"] is False


def test_add_no_verify_tls_is_silently_ignored_for_smb(fake_keyring: None) -> None:
    """SMB has no TLS setting; like any other backend-mismatched flag,
    ``--no-verify-tls`` is accepted and ignored."""
    invoke(
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

    show = invoke(["--json", "profile", "show", "demo"])
    report = json.loads(show.stdout)
    assert "verify_tls" not in report


def test_show_human_output_s3_with_verify_tls_disabled(fake_keyring: None) -> None:
    invoke(
        ["profile", "add", "demo", "--backend", "s3", "--bucket", "my-bucket", "--no-verify", "--no-verify-tls"],
        input="\n\n",
        exit_code=None,
    )
    result = invoke(["profile", "show", "demo"])
    assert result.output == snapshot("""\
name       : demo
backend    : s3
bucket     : my-bucket
endpoint   : (default)
region     : (default)
verify_tls : False
""")


def test_show_human_output_azure(fake_keyring: None) -> None:
    invoke(
        ["profile", "add", "demo", "--backend", "azure", "--container", "my-container", "--no-verify"],
        input="\n",
        exit_code=None,
    )
    result = invoke(["profile", "show", "demo"])
    assert result.output == snapshot("""\
name       : demo
backend    : azure
container  : my-container
account_url: (default)
verify_tls : True
""")


def test_show_human_output_smb(fake_keyring: None) -> None:
    invoke(
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
        exit_code=None,
    )
    result = invoke(["profile", "show", "demo"])
    # SMB has no TLS concept: no verify_tls line.
    assert result.output == snapshot("""\
name       : demo
backend    : smb
server     : nas.example.com
share      : backups
port       : 445
username   : admin
""")


# -- remove -----------------------------------------------------------------


def test_remove_with_force(fake_keyring: None) -> None:
    invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = invoke(["profile", "remove", "demo", "--force"])
    assert result.output == snapshot("removed profile 'demo'\n")

    listing = invoke(["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


def test_remove_without_force_prompts_and_can_be_declined(fake_keyring: None) -> None:
    invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = invoke(["profile", "remove", "demo"], input="n\n")
    assert result.output == snapshot("""\
Remove profile 'demo'? [y/N]: n
aborted
""")

    listing = invoke(["--json", "profile", "list"])
    assert json.loads(listing.stdout) == [{"name": "demo", "kind": "s3"}]


def test_remove_without_force_confirmed(fake_keyring: None) -> None:
    invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    invoke(["profile", "remove", "demo"], input="y\n")

    listing = invoke(["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


def test_remove_missing_profile_fails() -> None:
    result = invoke(["profile", "remove", "nope", "--force"], exit_code=1)
    assert result.output == snapshot("error: no such profile: 'nope'\n")


def test_remove_quiet_suppresses_the_removed_line(fake_keyring: None) -> None:
    invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = invoke(["--quiet", "profile", "remove", "demo", "--force"])
    assert result.output == ""

    listing = invoke(["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


def test_remove_no_input_without_force_fails_instead_of_prompting(fake_keyring: None) -> None:
    invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    result = invoke(["--no-input", "profile", "remove", "demo"], exit_code=1)
    assert result.output == snapshot("error: --no-input requires --force to remove without confirmation\n")

    listing = invoke(["--json", "profile", "list"])
    assert json.loads(listing.stdout) == [{"name": "demo", "kind": "s3"}]


def test_remove_no_input_with_force_skips_confirmation(fake_keyring: None) -> None:
    invoke(["profile", "add", "demo", "--backend", "s3", "--bucket", "b", "--no-verify"], input="\n\n")
    invoke(["--no-input", "profile", "remove", "demo", "--force"])

    listing = invoke(["--json", "profile", "list"])
    assert json.loads(listing.stdout) == []


# -- --no-input's stdin-is-a-terminal guard ----------------------------------


def test_require_piped_stdin_fails_fast_when_stdin_is_a_real_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    # CliRunner's stdin is never a tty, so no command-level test reaches this branch.
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    with pytest.raises(typer.Exit):
        profile_cmd._require_piped_stdin()
