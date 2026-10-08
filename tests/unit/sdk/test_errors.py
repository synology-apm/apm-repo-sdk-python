"""Unit tests for ``synology_apm_repo.sdk.errors``."""

from __future__ import annotations

import pickle

import pytest

from synology_apm_repo.sdk import errors
from synology_apm_repo.sdk.profiles import errors as profile_errors


def test_hierarchy_relationships() -> None:
    assert issubclass(errors.DataCorruptError, errors.FormatError)
    assert issubclass(errors.UnsupportedVersionError, errors.FormatError)
    assert issubclass(errors.ChunkCompactedError, errors.FormatError)
    assert issubclass(errors.KeyRequiredError, errors.KeyMaterialError)
    assert issubclass(errors.KeyMismatchError, errors.KeyMaterialError)
    for cls in (
        errors.FormatError,
        errors.KeyMaterialError,
        errors.NotFoundError,
        errors.UnsupportedDataFormatError,
    ):
        assert issubclass(cls, errors.ApmRepoError)


def test_none_of_the_hierarchy_shadows_builtin_keyerror() -> None:
    # A ``KeyError`` would be caught by unrelated ``except KeyError`` blocks.
    for name in dir(errors):
        obj = getattr(errors, name)
        if isinstance(obj, type) and issubclass(obj, Exception):
            assert obj.__name__ != "KeyError"
            assert not issubclass(obj, KeyError)


def test_message_includes_ref_and_spec() -> None:
    exc = errors.NotFoundError("no such file", ref="db/file_map", spec="FORMAT-SPEC.md: repo_info")
    text = str(exc)
    assert "no such file" in text
    assert "db/file_map" in text
    assert "FORMAT-SPEC.md: repo_info" in text
    assert exc.ref == "db/file_map"
    assert exc.spec == "FORMAT-SPEC.md: repo_info"


def test_message_without_ref_or_spec_is_just_the_message() -> None:
    exc = errors.ApmRepoError("plain message")
    assert str(exc) == "plain message"
    assert exc.ref is None
    assert exc.spec is None


def test_profile_not_found_is_a_not_found() -> None:
    exc = profile_errors.ProfileNotFoundError("no such profile", ref="my-profile")
    assert isinstance(exc, errors.NotFoundError)
    assert isinstance(exc, errors.ApmRepoError)
    assert "my-profile" in str(exc)


def test_profile_config_corrupt_is_not_repository_data_corruption() -> None:
    exc = profile_errors.ProfileConfigCorruptError("bad schema_version", ref="profiles.json")
    assert isinstance(exc, errors.ApmRepoError)
    assert not isinstance(exc, errors.FormatError)
    assert "profiles.json" in str(exc)


def test_content_unavailable_is_not_a_not_found_error() -> None:
    exc = errors.ContentUnavailableError("cloud-sync placeholder", ref="disk/Users/j/file.pdf")
    assert isinstance(exc, errors.ApmRepoError)
    # SDK call sites catch NotFoundError to skip an absent optional item;
    # this one must propagate past them.
    assert not isinstance(exc, errors.NotFoundError)
    assert not issubclass(errors.ContentUnavailableError, errors.NotFoundError)


def test_profile_secret_backend_unavailable_is_not_key_material_error() -> None:
    exc = profile_errors.ProfileSecretBackendUnavailableError("no usable OS keyring backend")
    assert isinstance(exc, errors.ApmRepoError)
    # KeyMaterialError is the repository's encryption key, unrelated to a
    # profile's OS-keyring secret.
    assert not isinstance(exc, errors.KeyMaterialError)
    assert not issubclass(profile_errors.ProfileSecretBackendUnavailableError, errors.KeyMaterialError)


def _all_error_classes() -> list[type[errors.ApmRepoError]]:
    return [
        obj
        for module in (errors, profile_errors)
        for obj in vars(module).values()
        if isinstance(obj, type) and issubclass(obj, errors.ApmRepoError) and obj.__module__ == module.__name__
    ]


@pytest.mark.parametrize("cls", _all_error_classes(), ids=lambda cls: cls.__name__)
def test_every_error_survives_a_pickle_round_trip(cls: type[errors.ApmRepoError]) -> None:
    """An export worker's failure reaches its parent process by pickle."""
    original = cls("bad thing", ref="a/b", spec="FORMAT-SPEC.md: X")

    copy = pickle.loads(pickle.dumps(original))

    assert type(copy) is cls
    assert (str(copy), copy.safe_message, copy.ref, copy.spec) == (
        str(original),
        original.safe_message,
        "a/b",
        "FORMAT-SPEC.md: X",
    )
