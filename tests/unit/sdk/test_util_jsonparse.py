"""Unit tests for ``synology_apm_repo.sdk._util.jsonparse``."""

from __future__ import annotations

import pytest

from synology_apm_repo.sdk._util.jsonparse import (
    parse_json_array,
    parse_json_object,
    try_parse_json_array,
    try_parse_json_object,
)
from synology_apm_repo.sdk.errors import DataCorruptError


class TestParseJsonObject:
    def test_parses_a_valid_object_from_str_or_bytes(self) -> None:
        assert parse_json_object('{"a": 1}', "spec") == {"a": 1}
        assert parse_json_object(b'{"a": 1}', "spec") == {"a": 1}

    def test_malformed_json_raises_data_corrupt_error_naming_the_value(self) -> None:
        with pytest.raises(DataCorruptError, match="spec did not parse as JSON"):
            parse_json_object("not json", "spec")

    def test_invalid_utf8_raises_data_corrupt_error(self) -> None:
        with pytest.raises(DataCorruptError, match="did not parse as JSON"):
            parse_json_object(b"\xff\xfe{", "spec")

    def test_integer_past_the_int_digit_limit_raises_data_corrupt_error(self) -> None:
        raw = '{"a": ' + "1" * 5000 + "}"
        with pytest.raises(DataCorruptError, match="spec did not parse as JSON"):
            parse_json_object(raw, "spec")
        assert try_parse_json_object(raw) is None

    @pytest.mark.parametrize("raw", ["[1, 2]", "3", '"s"', "null"])
    def test_a_non_object_top_level_value_raises(self, raw: str) -> None:
        with pytest.raises(DataCorruptError, match="expected an object"):
            parse_json_object(raw, "spec")

    def test_ref_is_carried_onto_the_raised_error(self) -> None:
        with pytest.raises(DataCorruptError, match="spec did not parse as JSON") as exc_info:
            parse_json_object("not json", "spec", ref="the-ref")
        assert exc_info.value.ref == "the-ref"


class TestParseJsonArray:
    def test_parses_a_valid_array(self) -> None:
        assert parse_json_array('["a", "b"]', "names") == ["a", "b"]

    @pytest.mark.parametrize("raw", ['{"a": 1}', '"abc"', "null"])
    def test_a_non_array_top_level_value_raises(self, raw: str) -> None:
        with pytest.raises(DataCorruptError, match="expected an array"):
            parse_json_array(raw, "names")


class TestTryParse:
    def test_parses_a_valid_object_or_array(self) -> None:
        assert try_parse_json_object('{"a": 1}') == {"a": 1}
        assert try_parse_json_array(b"[1, 2]") == [1, 2]

    @pytest.mark.parametrize("raw", ["not json", "[1, 2, 3]", "", None, 42, b"\xff\xfe"])
    def test_anything_but_a_json_object_is_none(self, raw: object) -> None:
        assert try_parse_json_object(raw) is None

    @pytest.mark.parametrize("raw", ["not json", '{"a": 1}', "", None])
    def test_anything_but_a_json_array_is_none(self, raw: object) -> None:
        assert try_parse_json_array(raw) is None
