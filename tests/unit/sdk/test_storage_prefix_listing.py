"""Unit tests for ``synology_apm_repo.sdk.storage.prefix_listing`` — the
shared listing/probing shapes ``S3Store``/``AzureStore`` both build their
own ``listdir``/``exists`` on top of. See ``test_storage_s3.py``'s/
``test_storage_azure.py``'s own ``exists()`` tests for end-to-end coverage
through a real backend; these are the pure-function pieces in isolation.
"""

from __future__ import annotations

import pytest

from synology_apm_repo.sdk.storage.prefix_listing import as_list_prefix, exists_via_prefix_probe, sorted_relative_names


class TestAsListPrefix:
    def test_empty_key_stays_empty_not_a_bare_slash(self) -> None:
        assert as_list_prefix("") == ""

    def test_non_empty_key_gets_a_trailing_slash(self) -> None:
        assert as_list_prefix("dir") == "dir/"

    def test_a_key_already_ending_in_slash_gets_a_second_one(self) -> None:
        # Callers always pass a bare key/path component here, never one
        # they've already slash-terminated themselves -- this documents
        # that as_list_prefix() itself doesn't guard against it.
        assert as_list_prefix("dir/") == "dir//"


class TestSortedRelativeNames:
    def test_trims_the_prefix_and_any_trailing_slash_then_sorts(self) -> None:
        raw = ["dir/b/", "dir/a.txt", "dir/c/"]
        assert sorted_relative_names(raw, "dir/") == ["a.txt", "b", "c"]

    def test_the_prefix_marker_itself_trims_to_empty_and_is_dropped(self) -> None:
        # A real "directory placeholder" object (a zero-byte key/blob
        # literally named "dir/") trims to "" -- listdir()'s own contract
        # is real child names only, never a placeholder entry for the
        # directory itself.
        raw = ["dir/", "dir/a.txt"]
        assert sorted_relative_names(raw, "dir/") == ["a.txt"]

    def test_empty_input_returns_empty_list(self) -> None:
        assert sorted_relative_names([], "dir/") == []

    def test_root_listing_uses_an_empty_prefix(self) -> None:
        assert sorted_relative_names(["a.txt", "b/"], "") == ["a.txt", "b"]


class _NotFoundError(Exception):
    pass


class _OtherError(Exception):
    pass


class TestExistsViaPrefixProbe:
    async def test_head_success_reports_true_without_ever_probing_the_prefix(self) -> None:
        probe_called = False

        async def probe_prefix() -> bool:
            nonlocal probe_called
            probe_called = True
            return True

        result = await exists_via_prefix_probe(
            head=lambda: _ok(), error_type=_NotFoundError, is_not_found=lambda exc: True, probe_prefix=probe_prefix
        )
        assert result is True
        assert probe_called is False

    async def test_head_not_found_falls_back_to_probe_prefix_result(self) -> None:
        async def probe_prefix_true() -> bool:
            return True

        async def probe_prefix_false() -> bool:
            return False

        result_true = await exists_via_prefix_probe(
            head=_raise_not_found,
            error_type=_NotFoundError,
            is_not_found=lambda exc: True,
            probe_prefix=probe_prefix_true,
        )
        result_false = await exists_via_prefix_probe(
            head=_raise_not_found,
            error_type=_NotFoundError,
            is_not_found=lambda exc: True,
            probe_prefix=probe_prefix_false,
        )
        assert result_true is True
        assert result_false is False

    async def test_head_error_not_recognized_as_not_found_reraises_without_probing(self) -> None:
        probe_called = False

        async def probe_prefix() -> bool:
            nonlocal probe_called
            probe_called = True
            return True

        with pytest.raises(_NotFoundError):
            await exists_via_prefix_probe(
                head=_raise_not_found,
                error_type=_NotFoundError,
                is_not_found=lambda exc: False,  # a permissions error, say -- never treated as "absent"
                probe_prefix=probe_prefix,
            )
        assert probe_called is False

    async def test_an_error_of_a_different_type_than_error_type_propagates_uncaught(self) -> None:
        async def head() -> object:
            raise _OtherError("unrelated failure")

        with pytest.raises(_OtherError):
            await exists_via_prefix_probe(
                head=head,
                error_type=_NotFoundError,  # _OtherError isn't this, so except clause never catches it
                is_not_found=lambda exc: True,
                probe_prefix=_unreachable_probe,
            )


async def _ok() -> object:
    return object()


async def _raise_not_found() -> object:
    raise _NotFoundError("not found")


async def _unreachable_probe() -> bool:
    raise AssertionError("probe_prefix must not be called")
