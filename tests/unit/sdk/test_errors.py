"""Unit tests for ``synology_apm_repo.sdk.errors``."""

from __future__ import annotations

from synology_apm_repo.sdk import errors


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
    # never name anything "KeyError" — it would be silently
    # caught by unrelated ``except KeyError`` blocks.
    for name in dir(errors):
        obj = getattr(errors, name)
        if isinstance(obj, type) and issubclass(obj, Exception):
            assert obj.__name__ != "KeyError"
            assert not issubclass(obj, KeyError)


def test_message_includes_ref_and_spec() -> None:
    exc = errors.NotFoundError("no such file", ref="db/file_map", spec="on-disk-format.md §1.2")
    text = str(exc)
    assert "no such file" in text
    assert "db/file_map" in text
    assert "on-disk-format.md §1.2" in text
    assert exc.ref == "db/file_map"
    assert exc.spec == "on-disk-format.md §1.2"


def test_message_without_ref_or_spec_is_just_the_message() -> None:
    exc = errors.ApmRepoError("plain message")
    assert str(exc) == "plain message"
    assert exc.ref is None
    assert exc.spec is None


def test_profile_not_found_is_a_not_found() -> None:
    exc = errors.ProfileNotFoundError("no such profile", ref="my-profile")
    assert isinstance(exc, errors.NotFoundError)
    assert isinstance(exc, errors.ApmRepoError)
    assert "my-profile" in str(exc)


def test_profile_config_corrupt_is_a_data_corrupt() -> None:
    exc = errors.ProfileConfigCorruptError("bad schema_version", ref="profiles.json")
    assert isinstance(exc, errors.DataCorruptError)
    assert isinstance(exc, errors.FormatError)
    assert "profiles.json" in str(exc)


def test_profile_secret_backend_unavailable_is_not_key_material_error() -> None:
    exc = errors.ProfileSecretBackendUnavailableError("no usable OS keyring backend")
    assert isinstance(exc, errors.ApmRepoError)
    # Deliberately NOT a KeyMaterialError: that hierarchy is about a dedup
    # repository's own encryption key, an unrelated "key" from a profile's
    # OS-keyring-backed secret.
    assert not isinstance(exc, errors.KeyMaterialError)
    assert not issubclass(errors.ProfileSecretBackendUnavailableError, errors.KeyMaterialError)
