"""Unit tests for ``synology_apm_repo.sdk.units.node_ref``."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from typing import Any, cast

import pytest
from hypothesis import given
from hypothesis import strategies as st

from synology_apm_repo.sdk.identifiers import CatalogId, VersionUid, WorkloadId, WorkloadUid
from synology_apm_repo.sdk.units.node_ref import (
    NodeRef,
    RefKind,
    ambiguous_matches,
    canonical_ref_for,
    disambiguate,
    disambiguate_catalogs,
    disambiguate_versions,
    disambiguate_workloads,
    match_display_name,
)


class TestStrAndParseRoundTrip:
    def test_simple_human_ref(self) -> None:
        ref = NodeRef.human("repo/path", "Test-Workload-02", "CORP-PC-001", "2026-08-07 09:00")
        text = str(ref)
        assert text == "repo/path#Test-Workload-02/CORP-PC-001/2026-08-07 09:00"
        assert NodeRef.parse(text) == ref

    def test_canonical_ref(self) -> None:
        ref = NodeRef.canonical(
            "repo/path",
            catalog_id=CatalogId("1"),
            workload_id=WorkloadId(2),
            version_uid=VersionUid("abc-uid"),
            extra=("Inbox", "Subject"),
        )
        text = str(ref)
        assert text == "repo/path#cat:1/wl:2/ver:abc-uid/Inbox/Subject"
        assert NodeRef.parse(text) == ref

    def test_raw_ref(self) -> None:
        ref = NodeRef.raw("repo/path", "VM-uid/dir/disk.img")
        assert str(ref) == "repo/path#raw/VM-uid/dir/disk.img"
        assert NodeRef.parse(str(ref)) == ref
        assert "/".join(ref.extra_segments) == "VM-uid/dir/disk.img"

    def test_segment_containing_hash_round_trips(self) -> None:
        ref = NodeRef.human("repo", "subject with # inside")
        text = str(ref)
        assert "%23" in text  # '#' is percent-encoded within the segment
        assert NodeRef.parse(text) == ref

    def test_segment_containing_slash_round_trips(self) -> None:
        # a single logical segment whose *content* happens to contain "/"
        # (e.g. a file_map path) must not be mistaken for multiple
        # segments once encoded and re-split.
        ref = NodeRef("repo", ("raw", "a/b/c"))
        text = str(ref)
        assert NodeRef.parse(text) == ref
        assert NodeRef.parse(text).extra_segments == ("a/b/c",)

    def test_segment_containing_percent_round_trips(self) -> None:
        ref = NodeRef.human("repo", "100%done")
        assert NodeRef.parse(str(ref)) == ref

    def test_segment_containing_control_char_round_trips(self) -> None:
        ref = NodeRef.human("repo", "line1\nline2\ttabbed")
        text = str(ref)
        assert "\n" not in text
        assert "\t" not in text
        assert NodeRef.parse(text) == ref

    def test_non_ascii_segment_round_trips(self) -> None:
        ref = NodeRef.human("repo", "日本語のファイル名", "emoji 🎉 name")
        assert NodeRef.parse(str(ref)) == ref

    def test_empty_segments_round_trip(self) -> None:
        ref = NodeRef("repo/path", ())
        assert str(ref) == "repo/path#"
        assert NodeRef.parse(str(ref)) == ref

    def test_parse_without_hash_raises(self) -> None:
        with pytest.raises(ValueError, match="missing '#'"):
            NodeRef.parse("no-hash-here")


class TestKindClassification:
    def test_canonical(self) -> None:
        ref = NodeRef("repo", ("cat:1", "wl:2", "ver:x"))
        assert ref.kind is RefKind.CANONICAL

    def test_raw(self) -> None:
        ref = NodeRef("repo", ("raw", "some/path"))
        assert ref.kind is RefKind.RAW

    def test_human_is_the_default(self) -> None:
        ref = NodeRef("repo", ("Test-Workload-02", "CORP-PC-001"))
        assert ref.kind is RefKind.HUMAN

    def test_empty_segments_is_human(self) -> None:
        assert NodeRef("repo", ()).kind is RefKind.HUMAN


class TestCanonicalIds:
    def test_extracts_ids_from_a_well_formed_canonical_ref(self) -> None:
        ref = NodeRef.canonical(
            "repo",
            catalog_id=CatalogId("1"),
            workload_id=WorkloadId(2),
            version_uid=VersionUid("abc-uid"),
            extra=("Inbox",),
        )
        assert ref.canonical_ids == (CatalogId("1"), 2, "abc-uid")

    def test_none_for_a_human_ref(self) -> None:
        ref = NodeRef.human("repo", "Test-Workload-02")
        assert ref.canonical_ids is None

    def test_none_for_a_malformed_canonical_prefix(self) -> None:
        # catalog_id is a plain string, never int-parsed, so only
        # workload_id can still be malformed this way.
        ref = NodeRef("repo", ("cat:1", "wl:notanumber", "ver:x"))
        assert ref.canonical_ids is None

    def test_none_when_canonical_prefix_is_truncated(self) -> None:
        ref = NodeRef("repo", ("cat:1",))
        assert ref.canonical_ids is None


class TestCanonicalRefFor:
    def test_builds_the_shared_prefix_from_repo_and_version(self) -> None:
        # canonical_ref_for's own contract: the (repo_root, repo_id,
        # ccid, workload_id, version_uid) prefix every provider's ref_for
        # shares, extra untouched. Only .layout.repo_root/.layout.repo_id/
        # .connection_config_id/.workload_id/.version_uid are ever
        # read, so lightweight stand-ins are enough -- no need to
        # construct a real DedupRepo/Version. repo_id=None mirrors a
        # vault's own RepoLayout, falling back to str(connection_config_id).
        repo = cast(Any, SimpleNamespace(layout=SimpleNamespace(repo_root="myrepo", repo_id=None)))
        version = cast(Any, SimpleNamespace(connection_config_id=7, workload_id=42, version_uid="v-abc"))

        ref = canonical_ref_for(repo, version, extra=("Inbox", "msg-1"))

        assert ref.canonical_ids == (CatalogId("7"), 42, "v-abc")
        assert ref.extra_segments == ("Inbox", "msg-1")
        assert str(ref).startswith("myrepo#")

    def test_extra_defaults_to_empty(self) -> None:
        repo = cast(Any, SimpleNamespace(layout=SimpleNamespace(repo_root="myrepo", repo_id=None)))
        version = cast(Any, SimpleNamespace(connection_config_id=1, workload_id=1, version_uid="v"))

        ref = canonical_ref_for(repo, version)

        assert ref.extra_segments == ()


class TestExtraSegments:
    def test_canonical_strips_the_fixed_prefix(self) -> None:
        ref = NodeRef.canonical(
            "repo",
            catalog_id=CatalogId("1"),
            workload_id=WorkloadId(2),
            version_uid=VersionUid("x"),
            extra=("Inbox", "Subject"),
        )
        assert ref.extra_segments == ("Inbox", "Subject")

    def test_raw_strips_the_raw_marker(self) -> None:
        ref = NodeRef.raw("repo", "VM-uid/disk.img")
        assert ref.extra_segments == ("VM-uid", "disk.img")

    def test_human_keeps_everything(self) -> None:
        ref = NodeRef.human("repo", "a", "b", "c")
        assert ref.extra_segments == ("a", "b", "c")


class TestChild:
    def test_appends_one_segment(self) -> None:
        ref = NodeRef.human("repo", "a", "b")
        assert ref.child("c") == NodeRef.human("repo", "a", "b", "c")

    def test_appends_several_segments_at_once(self) -> None:
        ref = NodeRef.human("repo", "a")
        assert ref.child("b", "c") == NodeRef.human("repo", "a", "b", "c")

    def test_no_extra_segments_returns_an_equal_but_new_ref(self) -> None:
        ref = NodeRef.human("repo", "a")
        assert ref.child() == ref

    def test_original_ref_is_untouched(self) -> None:
        ref = NodeRef.human("repo", "a")
        ref.child("b")
        assert ref == NodeRef.human("repo", "a")

    def test_works_on_a_canonical_ref_the_same_way(self) -> None:
        ref = NodeRef.canonical(
            "repo", catalog_id=CatalogId("1"), workload_id=WorkloadId(2), version_uid=VersionUid("x")
        )
        child = ref.child("fs", "p0")
        assert child.repo_path == "repo"
        assert child.segments == ("cat:1", "wl:2", "ver:x", "fs", "p0")


class TestDisambiguate:
    def test_no_collision_leaves_names_unchanged(self) -> None:
        result = disambiguate([("Alice", "id-1"), ("Bob", "id-2")])
        assert result == ["Alice", "Bob"]

    def test_collision_appends_a_short_hash_suffix(self) -> None:
        result = disambiguate([("2026-08-07 09:00", "uid-a"), ("2026-08-07 09:00", "uid-b")])
        assert result[0] != result[1]
        assert result[0].startswith("2026-08-07 09:00 #")
        assert result[1].startswith("2026-08-07 09:00 #")
        assert len(result[0]) == len("2026-08-07 09:00 #") + 4

    def test_suffix_is_derived_from_the_stable_id_not_the_name(self) -> None:
        # same id -> same suffix even if paired with a different name string
        first = disambiguate([("Name A", "same-id"), ("Name A", "other-id")])[0]
        second = disambiguate([("Name B", "same-id"), ("Name B", "other-id")])[0]
        assert first.rsplit("#", 1)[1] == second.rsplit("#", 1)[1]

    def test_only_the_colliding_names_get_a_suffix(self) -> None:
        result = disambiguate([("dup", "id-1"), ("dup", "id-2"), ("unique", "id-3")])
        assert result[2] == "unique"
        assert result[0] != "dup"
        assert result[1] != "dup"

    # -- hints (a human-meaningful reason for the collision,
    # e.g. workload sub_type) preferred over the opaque hash suffix --

    def test_hint_resolves_the_collision_instead_of_a_hash(self) -> None:
        result = disambiguate(
            [("Alice Example <a@x>", "wl-1"), ("Alice Example <a@x>", "wl-2")],
            hints=["MAIL", "DRIVE"],
        )
        assert result == ["Alice Example <a@x> · MAIL", "Alice Example <a@x> · DRIVE"]
        assert not any("#" in name for name in result)

    def test_missing_hint_leaves_that_entry_bare_when_the_hint_alone_already_resolves_it(self) -> None:
        # entry 1 gets "dup · MAIL"; entry 2 has no hint so stays "dup" —
        # and that's already unique (no candidate else equals bare "dup"),
        # so no hash is needed at all. This is the algorithm correctly
        # noticing the hinted candidate no longer collides with anything,
        # not a case that still needs a hash fallback.
        result = disambiguate(
            [("dup", "id-1"), ("dup", "id-2")],
            hints=["MAIL", None],
        )
        assert result == ["dup · MAIL", "dup"]

    def test_two_entries_sharing_both_name_and_hint_still_fall_back_to_a_hash(self) -> None:
        # collision-safety must never be weakened by a hint that doesn't
        # actually resolve it (two different real workloads happen to
        # share a sub_type too, e.g. two MAIL accounts).
        result = disambiguate(
            [("dup", "id-1"), ("dup", "id-2")],
            hints=["MAIL", "MAIL"],
        )
        assert result[0] != result[1]
        assert result[0].startswith("dup · MAIL #")
        assert result[1].startswith("dup · MAIL #")

    def test_hints_omitted_equals_hints_none(self) -> None:
        with_hints_omitted = disambiguate([("dup", "id-1"), ("dup", "id-2"), ("unique", "id-3")])
        with_hints_none = disambiguate([("dup", "id-1"), ("dup", "id-2"), ("unique", "id-3")], hints=None)
        assert with_hints_omitted == with_hints_none

    def test_hint_does_not_affect_non_colliding_names(self) -> None:
        result = disambiguate([("solo", "id-1")], hints=["MAIL"])
        assert result == ["solo"]


class TestDisambiguateCatalogsWorkloadsVersions:
    """``disambiguate_catalogs``/``disambiguate_workloads``/
    ``disambiguate_versions`` — the "build pairs, ``disambiguate()``" step
    ``catalog_pairs``/``workload_pairs``/``version_pairs`` each fed
    independently in the CLI and the browser before these existed.
    ``TestDisambiguate`` above already covers ``disambiguate()``'s own
    collision/hint/hash logic in full; these only check that each
    wrapper builds the right pairs (and, for workloads, the right hint)
    from real-shaped objects and returns names in the same order."""

    def test_disambiguate_catalogs_disambiguates_colliding_display_names(self) -> None:
        catalogs = [
            SimpleNamespace(display_name="dup", catalog_id=CatalogId("cat-1")),
            SimpleNamespace(display_name="dup", catalog_id=CatalogId("cat-2")),
            SimpleNamespace(display_name="unique", catalog_id=CatalogId("cat-3")),
        ]
        result = disambiguate_catalogs(cast(Any, catalogs))
        assert result[2] == "unique"
        assert result[0] != result[1]
        assert result[0].startswith("dup #") and result[1].startswith("dup #")

    def test_disambiguate_workloads_uses_type_hint_by_default(self) -> None:
        workloads = [
            SimpleNamespace(display_name="Alice", workload_uid=WorkloadUid("wl-1"), type_hint="MAIL"),
            SimpleNamespace(display_name="Alice", workload_uid=WorkloadUid("wl-2"), type_hint="DRIVE"),
        ]
        result = disambiguate_workloads(cast(Any, workloads))
        assert result == ["Alice · MAIL", "Alice · DRIVE"]

    def test_disambiguate_workloads_use_type_hint_false_skips_the_hint(self) -> None:
        """The browser's own per-sub_type leaf list: every sibling already
        shares one ``type_hint`` by construction, so showing it again
        would be redundant -- ``use_type_hint=False`` falls back straight
        to the hash suffix instead, same as passing no hints at all."""
        workloads = [
            SimpleNamespace(display_name="Alice", workload_uid=WorkloadUid("wl-1"), type_hint="MAIL"),
            SimpleNamespace(display_name="Alice", workload_uid=WorkloadUid("wl-2"), type_hint="MAIL"),
        ]
        result = disambiguate_workloads(cast(Any, workloads), use_type_hint=False)
        assert "·" not in result[0] and "·" not in result[1]
        assert result[0] != result[1]
        assert result[0].startswith("Alice #") and result[1].startswith("Alice #")

    def test_disambiguate_versions_disambiguates_colliding_display_names(self) -> None:
        versions = [
            SimpleNamespace(display_name="2026-01-01 00:00", version_uid=VersionUid("v-1")),
            SimpleNamespace(display_name="2026-01-01 00:00", version_uid=VersionUid("v-2")),
        ]
        result = disambiguate_versions(cast(Any, versions))
        assert result[0] != result[1]
        assert result[0].startswith("2026-01-01 00:00 #") and result[1].startswith("2026-01-01 00:00 #")


class TestMatchDisplayName:
    def test_finds_unique_name(self) -> None:
        assert match_display_name("foo", [("foo", "1"), ("bar", "2")], ["obj-foo", "obj-bar"]) == "obj-foo"

    def test_returns_none_when_not_found(self) -> None:
        assert match_display_name("baz", [("foo", "1")], ["obj-foo"]) is None

    def test_requires_disambiguation_suffix_on_collision(self) -> None:
        pairs = [("dup", "id1"), ("dup", "id2")]
        objects = ["obj1", "obj2"]
        assert match_display_name("dup", pairs, objects) is None  # bare name no longer matches either
        suffixed = match_display_name("dup #" + hashlib.sha256(b"id1").hexdigest()[:4], pairs, objects)
        assert suffixed == "obj1"

    def test_hints_resolve_a_collision_the_same_way_disambiguate_does(self) -> None:
        # hints must match whatever the display side passed to its own
        # disambiguate() call for these same pairs -- e.g. resolving
        # "Alice Example <a@x> · MAIL" back to the Mail workload, not
        # just any workload sharing that display name.
        pairs = [("Alice Example <a@x>", "wl-1"), ("Alice Example <a@x>", "wl-2")]
        objects = ["mail-workload", "drive-workload"]

        assert match_display_name("Alice Example <a@x> · MAIL", pairs, objects, hints=["MAIL", "DRIVE"]) == (
            "mail-workload"
        )
        assert match_display_name("Alice Example <a@x> · DRIVE", pairs, objects, hints=["MAIL", "DRIVE"]) == (
            "drive-workload"
        )
        # the bare (un-hinted) name no longer matches either, same as the hash case
        assert match_display_name("Alice Example <a@x>", pairs, objects, hints=["MAIL", "DRIVE"]) is None


class TestAmbiguousMatches:
    def test_empty_when_target_genuinely_missing(self) -> None:
        assert ambiguous_matches("baz", [("foo", "1")]) == []

    def test_returns_disambiguated_candidates_on_collision(self) -> None:
        pairs = [("dup", "id1"), ("dup", "id2")]
        candidates = ambiguous_matches("dup", pairs)
        assert candidates == [
            "dup #" + hashlib.sha256(b"id1").hexdigest()[:4],
            "dup #" + hashlib.sha256(b"id2").hexdigest()[:4],
        ]

    def test_reports_hint_resolved_names_when_hints_given(self) -> None:
        # the *raw* (pre-suffix) name is what's compared against target --
        # both rows still share it even though a hint resolved each one's
        # own disambiguated form differently, so both come back as real,
        # actionable candidates (not narrowed away by the hint).
        pairs = [("Alice Example <a@x>", "wl-1"), ("Alice Example <a@x>", "wl-2")]
        assert ambiguous_matches("Alice Example <a@x>", pairs, hints=["MAIL", "DRIVE"]) == [
            "Alice Example <a@x> · MAIL",
            "Alice Example <a@x> · DRIVE",
        ]


# -- property-based round-trip test --

_segment_text = st.text(alphabet=st.characters(min_codepoint=0x20, max_codepoint=0x2FFFF), max_size=40)


@given(
    repo_path=st.text(min_size=1, max_size=20).filter(lambda s: "#" not in s),
    # a *lone* empty-string segment is the one documented, unrepresentable
    # edge case (indistinguishable from zero segments — both encode to
    # ""); excluded here as out of scope for this property, not ignored.
    segments=st.lists(_segment_text, max_size=6).filter(lambda s: s != [""]),
)
def test_str_parse_round_trip_property(repo_path: str, segments: list[str]) -> None:
    ref = NodeRef(repo_path, tuple(segments))
    assert NodeRef.parse(str(ref)) == ref
