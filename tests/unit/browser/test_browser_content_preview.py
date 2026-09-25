"""Unit tests for ``browser.content_preview`` — pure-function coverage,
no Textual/Pilot needed. The
Pilot test (``tests/integration/browser/test_browser_pilot_preview.py``)
covers the real, end-to-end version wired into ``UnitScreen``'s detail
pane against real recorded sample data."""

from __future__ import annotations

import json

from synology_apm_repo.browser.content_preview import (
    _drop_trailing_unterminated_tag,
    render_calendar_event_preview,
    render_contact_preview,
    render_html_preview,
    render_mail_preview,
    render_teams_chat_preview,
    visible_site_fields,
)
from synology_apm_repo.sdk.units.content.saas_calendar import build_ics
from synology_apm_repo.sdk.units.content.saas_contact import build_contact_csv
from synology_apm_repo.sdk.units.content.saas_teams_chat import render_channel_html

_PLAIN_EML = (
    b"From: Alice <alice@example.com>\r\n"
    b"To: Bob <bob@example.com>\r\n"
    b"Subject: Hello there\r\n"
    b"Date: Mon, 07 Aug 2026 09:00:00 +0000\r\n"
    b"Content-Type: text/plain; charset=utf-8\r\n"
    b"\r\n"
    b"Hi Bob,\r\n\r\nThis is the body.\r\n\r\nCheers,\r\nAlice\r\n"
)

_HTML_EML = (
    b"From: Alice <alice@example.com>\r\n"
    b"Subject: An HTML mail\r\n"
    b"Content-Type: text/html; charset=utf-8\r\n"
    b"\r\n"
    b"<html><body><style>.x{color:red}</style>"
    b"<p>Hello <b>Bob</b></p><p>Second paragraph</p></body></html>\r\n"
)

_NO_HEADERS_EML = b"Content-Type: text/plain; charset=utf-8\r\n\r\nJust a body, no headers.\r\n"

_EMPTY_SUBJECT_EML = b"From: Alice <alice@example.com>\r\nSubject:\r\n\r\nBody text.\r\n"


class TestRenderMailPreview:
    def test_plain_text_mail_shows_headers_and_body(self) -> None:
        text = render_mail_preview(_PLAIN_EML)
        assert "From: Alice <alice@example.com>" in text
        assert "To: Bob <bob@example.com>" in text
        assert "Subject: Hello there" in text
        assert "This is the body." in text

    def test_html_mail_body_has_tags_stripped(self) -> None:
        text = render_mail_preview(_HTML_EML)
        assert "Hello" in text and "Bob" in text
        assert "Second paragraph" in text
        assert "<p>" not in text and "<b>" not in text
        # <style> contents must never leak into the rendered text.
        assert "color:red" not in text and ".x{" not in text

    def test_missing_from_to_headers_are_simply_omitted_not_an_error(self) -> None:
        text = render_mail_preview(_NO_HEADERS_EML)
        assert "Just a body, no headers." in text
        assert "From:" not in text
        assert "To:" not in text

    def test_missing_subject_shows_the_no_subject_placeholder_not_omitted(self) -> None:
        # Unlike From/To/Date, Subject is never dropped -- a missing one
        # must show the placeholder, not disappear from the preview.
        text = render_mail_preview(_NO_HEADERS_EML)
        assert "Subject: (no subject)" in text

    def test_empty_subject_header_shows_the_no_subject_placeholder(self) -> None:
        # A present-but-empty Subject header is a different real case from
        # a wholly missing one -- both must show the same placeholder.
        text = render_mail_preview(_EMPTY_SUBJECT_EML)
        assert "Subject: (no subject)" in text

    def test_long_body_is_truncated_with_a_note(self) -> None:
        long_body = b"From: Alice <a@x.com>\r\nSubject: Long\r\n\r\n" + b"x" * 20_000
        text = render_mail_preview(long_body, max_chars=100)
        assert len(text) < 20_000
        assert "truncated" in text.lower()
        assert "export" in text.lower()

    def test_short_content_is_not_truncated(self) -> None:
        text = render_mail_preview(_PLAIN_EML, max_chars=4000)
        assert "truncated" not in text.lower()


_MESSAGE_ROWS = [
    {
        "author": '{"name": "Alice Example"}',
        "create_time": 1700000000,
        "content_preview": "general 1",
        "metadata": None,
        "is_sys_message": 0,
        "reply_to_id": None,
    },
    {
        "author": '{"name": "someone else"}',
        "create_time": 1700000100,
        "content_preview": "general 2",
        "metadata": None,
        "is_sys_message": 0,
        "reply_to_id": None,
    },
]


class TestRenderHtmlPreview:
    def test_recognizes_and_flattens_a_real_channel_export(self) -> None:
        # render_channel_html is the actual SDK function that produces the
        # one real, self-contained HTML document this preview exists to
        # handle (TeamsChatProvider's channel export) — using it directly
        # here (not a hand-rolled HTML fixture) means this test breaks if
        # the two ever drift out of sync with each other.
        html = render_channel_html(_MESSAGE_ROWS, channel_name="General").encode("utf-8")
        text = render_html_preview(html)
        assert text is not None
        assert "Alice Example" in text
        assert "general 1" in text
        assert "general 2" in text
        assert "<div" not in text and "<html" not in text and "<style" not in text

    def test_non_html_bytes_return_none(self) -> None:
        assert render_html_preview(b"\x00\x01\x02 random binary junk") is None
        assert render_html_preview(b"just plain text, no markup at all") is None

    def test_tolerates_a_leading_bom_and_whitespace(self) -> None:
        html = b"\xef\xbb\xbf   \n<!doctype html><html><body><p>hi</p></body></html>"
        text = render_html_preview(html)
        assert text is not None
        assert "hi" in text

    def test_long_html_is_truncated_with_a_note(self) -> None:
        html = b"<!doctype html><html><body>" + (b"<p>x</p>" * 5000) + b"</body></html>"
        text = render_html_preview(html, max_chars=100)
        assert text is not None
        assert len(text) < 5000
        assert "truncated" in text.lower()

    def test_html_that_flattens_to_nothing_returns_none(self) -> None:
        # A syntactically-HTML document with no actual text content (only
        # markup/style) has nothing worth previewing.
        html = b"<!doctype html><html><head><style>.x{color:red}</style></head><body></body></html>"
        assert render_html_preview(html) is None

    def test_a_read_window_truncated_mid_attribute_shows_a_placeholder_not_raw_base64(self) -> None:
        """Simulates a large inline base64 image (e.g. a Teams sticker's
        ``<img src="data:image/jpeg;base64,...">``) that exceeds
        ``UnitScreen``'s size-bounded read window, cutting off mid-attribute
        with no closing quote or tag."""
        fake_base64 = "AAAA" * 20_000  # ~80,000 chars, well past max_chars — would dominate the preview if leaked
        html = (
            b'<!doctype html><html><body><div class="msg"><div class="hdr">Snow Jon</div>'
            b'<div class="body">flower~~</div></div><div class="msg"><img class="sticker" '
            b'alt="[sticker]" src="data:image/jpeg;base64,' + fake_base64.encode()
        )
        text = render_html_preview(html, max_chars=4000)
        assert text is not None
        assert "flower~~" in text
        assert "AAAA" not in text
        assert "[image, ≥" in text
        assert "not shown in preview" in text

    def test_a_truncated_non_img_tag_gets_a_generic_placeholder_label(self) -> None:
        fake_attr = "x" * 5000
        html = b'<!doctype html><html><body><p>hi</p><div data-blob="' + fake_attr.encode()
        text = render_html_preview(html, max_chars=4000)
        assert text is not None
        assert "hi" in text
        assert "[content, ≥" in text

    def test_trailing_plain_text_after_the_last_real_tag_is_not_mistaken_for_a_truncation(self) -> None:
        # No incomplete tag here at all — just prose after the last real
        # closing tag (e.g. a read window that happens to end mid-word in
        # ordinary text) — must not get a spurious placeholder.
        html = b"<!doctype html><html><body><p>hello</p> trailing plain text with no tag at all"
        text = render_html_preview(html, max_chars=4000)
        assert text is not None
        assert "[" not in text

    def test_a_window_ending_on_a_real_tag_boundary_is_unaffected(self) -> None:
        # The trim-to-last-> defense must not cut anything when the
        # window already ends cleanly — the common case (most real
        # units are smaller than the read cap and this never triggers).
        html = b"<!doctype html><html><body><p>hello</p><p>world</p></body></html>"
        text = render_html_preview(html)
        assert text is not None
        assert "hello" in text
        assert "world" in text


class TestRenderCalendarEventPreview:
    def test_a_fully_populated_event_shows_all_six_fields(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "id": "event-1",
                    "iCalUID": "event-1@example.com",
                    "summary": "Standup",
                    "location": "Room 1",
                    "start": {"dateTime": "2026-01-01T09:00:00+00:00"},
                    "end": {"dateTime": "2026-01-01T09:30:00+00:00"},
                    "organizer": {"email": "boss@example.com"},
                    "recurrence": ["RRULE:FREQ=WEEKLY"],
                }
            }
        ).encode()
        ics = build_ics(meta, "event-1")
        text = render_calendar_event_preview(ics)
        assert text == (
            "Organizer: boss@example.com\n"
            "Title: Standup\n"
            "Location: Room 1\n"
            "Start Time: 2026-01-01 09:00:00+00:00\n"
            "End Time: 2026-01-01 09:30:00+00:00\n"
            "Recurrence: FREQ=WEEKLY"
        )

    def test_empty_summary_shows_no_title_not_the_raw_id(self) -> None:
        # An empty summary must show "(no title)", never the raw event
        # id, since the .ics UID also falls back to event_id.
        meta = json.dumps({"client_metadata": {"id": "AAMkAGEzYWI2", "summary": ""}}).encode()
        ics = build_ics(meta, "AAMkAGEzYWI2")
        text = render_calendar_event_preview(ics)
        assert "Title: (no title)" in text
        assert "AAMkAGEzYWI2" not in text

    def test_missing_optional_fields_show_the_none_placeholder(self) -> None:
        meta = json.dumps({"client_metadata": {"id": "event-2", "summary": "Bare event"}}).encode()
        ics = build_ics(meta, "event-2")
        text = render_calendar_event_preview(ics)
        assert text == (
            "Organizer: (none)\nTitle: Bare event\nLocation: (none)\nStart Time: (none)\nEnd Time: (none)\n"
            "Recurrence: (none)"
        )

    def test_an_all_day_event_shows_a_bare_date_not_a_time(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "id": "event-3",
                    "summary": "All day",
                    "start": {"date": "2026-03-01"},
                    "end": {"date": "2026-03-02"},
                }
            }
        ).encode()
        ics = build_ics(meta, "event-3")
        text = render_calendar_event_preview(ics)
        assert "Start Time: 2026-03-01" in text
        assert "End Time: 2026-03-02" in text

    def test_no_vevent_at_all_shows_the_none_placeholder(self) -> None:
        # A hand-built .ics with no VEVENT at all -- build_ics() (used by
        # every other test in this class) always emits exactly one, so
        # this is the one way to reach the "genuinely nothing to show"
        # fallback rather than a real, if bare, event.
        ics = b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n"
        assert render_calendar_event_preview(ics) == "(none)"


class TestDropTrailingUnterminatedTag:
    def test_no_greater_than_character_at_all_returns_data_unchanged(self) -> None:
        data = b"plain text with no angle brackets whatsoever"
        assert _drop_trailing_unterminated_tag(data) == (data, None)


class TestRenderContactPreview:
    def test_m365_csv_shows_full_name_and_email(self) -> None:
        meta = json.dumps(
            {"client_metadata": {"givenName": "Alice", "surname": "Wu", "emailAddresses": [{"address": "alice@x.com"}]}}
        ).encode()
        csv_bytes = build_contact_csv(meta)
        text = render_contact_preview(csv_bytes)
        assert text == "Full Name: Alice Wu\nEmail: alice@x.com"

    def test_m365_csv_with_no_name_or_email_shows_the_none_placeholder(self) -> None:
        csv_bytes = build_contact_csv(json.dumps({"client_metadata": {}}).encode())
        text = render_contact_preview(csv_bytes)
        assert text == "Full Name: (none)\nEmail: (none)"

    def test_gws_json_shows_full_name_and_email(self) -> None:
        # Google's People API nests names/emails as lists of objects,
        # unlike M365's flat CSV columns above.
        data = json.dumps(
            {
                "client_metadata": {
                    "names": [{"displayName": "Alice Example", "givenName": "Alice", "familyName": "Example"}],
                    "emailAddresses": [{"value": "alice@gwsdemo.example.com"}],
                }
            }
        ).encode()
        text = render_contact_preview(data)
        assert text == "Full Name: Alice Example\nEmail: alice@gwsdemo.example.com"

    def test_gws_json_with_no_name_or_email_shows_the_none_placeholder(self) -> None:
        data = json.dumps({"client_metadata": {}}).encode()
        text = render_contact_preview(data)
        assert text == "Full Name: (none)\nEmail: (none)"

    def test_m365_csv_shows_every_present_optional_field(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "givenName": "Alice",
                    "surname": "Wu",
                    "emailAddresses": [{"address": "alice@x.com"}],
                    "jobTitle": "Engineer",
                    "companyName": "Acme",
                    "businessPhones": ["555-1000"],
                    "homePhones": ["555-2000"],
                    "mobilePhone": "555-3000",
                    "businessAddress": {
                        "street": "1 Main St",
                        "city": "Springfield",
                        "state": "IL",
                        "postalCode": "62704",
                        "countryOrRegion": "US",
                    },
                    "personalNotes": "met at conference",
                }
            }
        ).encode()
        csv_bytes = build_contact_csv(meta)
        text = render_contact_preview(csv_bytes)
        assert text == (
            "Full Name: Alice Wu\n"
            "Email: alice@x.com\n"
            "Job Title: Engineer\n"
            "Company: Acme\n"
            "Business Phone: 555-1000\n"
            "Home Phone: 555-2000\n"
            "Mobile Phone: 555-3000\n"
            "Address: 1 Main St, Springfield IL, 62704, US\n"
            "Notes: met at conference"
        )

    def test_gws_json_shows_every_present_optional_field(self) -> None:
        # Real GWS contact field shapes: organizations[0].{name,title},
        # phoneNumbers[0].value (no "type" in real data), biographies[0].value,
        # birthdays[0].text.
        data = json.dumps(
            {
                "client_metadata": {
                    "names": [{"displayName": "a bc"}],
                    "emailAddresses": [{"value": "a@x.com"}],
                    "organizations": [{"name": "syno", "title": "bartender"}],
                    "phoneNumbers": [{"value": "08000000123"}],
                    "birthdays": [{"date": {"day": 11, "month": 10, "year": 1996}, "text": "10/11/1996"}],
                    "biographies": [{"value": "likes cats"}],
                }
            }
        ).encode()
        text = render_contact_preview(data)
        assert text == (
            "Full Name: a bc\n"
            "Email: a@x.com\n"
            "Job Title: bartender\n"
            "Company: syno\n"
            "Phone: 08000000123\n"
            "Birthday: 10/11/1996\n"
            "Notes: likes cats"
        )

    def test_gws_phone_number_with_a_real_type_gets_a_labeled_line(self) -> None:
        data = json.dumps(
            {"client_metadata": {"phoneNumbers": [{"value": "555-1000", "formattedType": "Mobile"}]}}
        ).encode()
        text = render_contact_preview(data)
        assert "Phone (Mobile): 555-1000" in text

    def test_gws_address_prefers_the_formatted_value_over_composing_components(self) -> None:
        data = json.dumps(
            {
                "client_metadata": {
                    "addresses": [{"formattedValue": "1 Main St, Springfield", "streetAddress": "should not be used"}]
                }
            }
        ).encode()
        text = render_contact_preview(data)
        assert "Address: 1 Main St, Springfield" in text

    def test_gws_address_composes_from_components_when_no_formatted_value(self) -> None:
        data = json.dumps(
            {"client_metadata": {"addresses": [{"streetAddress": "1 Main St", "city": "Springfield"}]}}
        ).encode()
        text = render_contact_preview(data)
        assert "Address: 1 Main St, Springfield" in text

    def test_gws_birthday_falls_back_to_composing_from_date_when_no_text(self) -> None:
        data = json.dumps(
            {"client_metadata": {"birthdays": [{"date": {"year": 1996, "month": 10, "day": 11}}]}}
        ).encode()
        text = render_contact_preview(data)
        assert "Birthday: 1996-10-11" in text

    def test_gws_birthday_without_a_year_omits_it(self) -> None:
        data = json.dumps({"client_metadata": {"birthdays": [{"date": {"month": 10, "day": 11}}]}}).encode()
        text = render_contact_preview(data)
        assert "Birthday: 10-11" in text

    def test_gws_birthday_with_year_only_shows_just_the_year(self) -> None:
        data = json.dumps({"client_metadata": {"birthdays": [{"date": {"year": 1996}}]}}).encode()
        text = render_contact_preview(data)
        assert "Birthday: 1996" in text

    def test_gws_birthday_with_month_and_day_as_the_unspecified_sentinel_shows_just_the_year(self) -> None:
        """The People API's own "unspecified" sentinel for a repeated
        Date field's ``month``/``day`` is ``0``, not an absent key --
        real GWS data for a year-only birthday. Must not render the
        nonsensical "1996-00-00"."""
        data = json.dumps({"client_metadata": {"birthdays": [{"date": {"year": 1996, "month": 0, "day": 0}}]}}).encode()
        text = render_contact_preview(data)
        assert "Birthday: 1996" in text
        assert "00" not in text

    def test_gws_birthday_with_a_known_month_and_unspecified_day_still_shows_the_month(self) -> None:
        """A known month with the People API's own ``day=0``
        "unspecified" sentinel must still show the month, not fall all
        the way back to year-only."""
        data = json.dumps({"client_metadata": {"birthdays": [{"date": {"year": 1990, "month": 6, "day": 0}}]}}).encode()
        text = render_contact_preview(data)
        assert "Birthday: 1990-06" in text

    def test_gws_birthday_with_the_unspecified_year_sentinel_omits_the_year(self) -> None:
        """Regression test: ``year=0`` is the People API's own
        "unspecified" sentinel too (same as ``month``/``day``'s), so a
        year-omitted birthday must not render the nonsensical
        "0-06-15"."""
        data = json.dumps({"client_metadata": {"birthdays": [{"date": {"year": 0, "month": 6, "day": 15}}]}}).encode()
        text = render_contact_preview(data)
        assert "Birthday: 06-15" in text
        assert "0-06-15" not in text

    def test_gws_birthday_with_a_known_day_and_unspecified_month_still_shows_the_day(self) -> None:
        """A known day with no month (and no year) must still render
        that one real field, not nothing."""
        data = json.dumps({"client_metadata": {"birthdays": [{"date": {"day": 15}}]}}).encode()
        text = render_contact_preview(data)
        assert "Birthday: 15" in text

    def test_gws_birthday_with_no_text_and_no_usable_date_at_all_is_omitted_entirely(self) -> None:
        data = json.dumps({"client_metadata": {"birthdays": [{"date": {}}]}}).encode()
        text = render_contact_preview(data)
        assert "Birthday" not in text

    def test_gws_phone_numbers_not_a_list_are_ignored(self) -> None:
        data = json.dumps({"client_metadata": {"phoneNumbers": "not-a-list"}}).encode()
        text = render_contact_preview(data)
        assert "Phone" not in text

    def test_gws_phone_number_entries_that_are_not_objects_are_skipped(self) -> None:
        data = json.dumps({"client_metadata": {"phoneNumbers": ["not-an-object", {"value": "555-1000"}]}}).encode()
        text = render_contact_preview(data)
        assert "Phone: 555-1000" in text

    def test_gws_phone_number_with_no_real_value_is_skipped(self) -> None:
        data = json.dumps({"client_metadata": {"phoneNumbers": [{"value": ""}, {"value": "555-1000"}]}}).encode()
        text = render_contact_preview(data)
        assert text.count("Phone") == 1
        assert "Phone: 555-1000" in text


class TestVisibleSiteFields:
    """Direct unit tests for ``visible_site_fields`` — no test anywhere
    in this package gives it its own direct coverage, unlike every
    sibling ``render_*`` function."""

    def test_drops_odata_prefixed_keys_case_insensitively(self) -> None:
        values: dict[str, object] = {"odata.type": "x", "OData__UIVersionString": "y", "Title": "Real"}
        assert visible_site_fields(values) == {"Title": "Real"}

    def test_drops_keys_ending_in_id_case_sensitively(self) -> None:
        values: dict[str, object] = {"AuthorId": 1, "ID": 2, "Title": "Real"}
        assert visible_site_fields(values) == {"ID": 2, "Title": "Real"}

    def test_drops_guid_exactly(self) -> None:
        values: dict[str, object] = {"GUID": "abc-123", "Title": "Real"}
        assert visible_site_fields(values) == {"Title": "Real"}

    def test_preserves_insertion_order_of_surviving_keys(self) -> None:
        values = {"Title": "a", "odata.type": "x", "Modified": "b", "AuthorId": 1, "Status": "c"}
        assert list(visible_site_fields(values)) == ["Title", "Modified", "Status"]


class TestRenderTeamsChatPreview:
    """Every case here builds its HTML via the real ``render_channel_html``
    (not a hand-rolled fixture) — the same reasoning
    ``TestRenderHtmlPreview.test_recognizes_and_flattens_a_real_channel_export``
    already gives: this breaks if the two ever drift out of sync."""

    def test_sender_and_timestamp_sit_directly_above_the_body_no_gap(self) -> None:
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "hello there",
                "metadata": None,
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html)
        lines = text.splitlines()
        hdr_index = next(i for i, line in enumerate(lines) if "Alice" in line and "·" in line)
        assert lines[hdr_index + 1] == "hello there"
        # The avatar's own single-letter placeholder never appears as its
        # own line -- pure visual noise in a text rendering.
        assert "A" not in lines

    def test_date_separator_appears_as_its_own_block(self) -> None:
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "hi",
                "metadata": None,
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html)
        assert any(line.startswith("──") and line.endswith("──") for line in text.splitlines())

    def test_reply_quote_sits_directly_above_the_replying_messages_header(self) -> None:
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "original message",
                "metadata": None,
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": "1",
            },
            {
                "author": '{"name": "Bob"}',
                "create_time": 1700000100,
                "content_preview": "reply text",
                "metadata": None,
                "is_sys_message": 0,
                "reply_to_id": "1",
                "msg_id": "2",
            },
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html)
        lines = text.splitlines()
        reply_index = next(i for i, line in enumerate(lines) if "replying to" in line)
        assert "Alice" in lines[reply_index]
        assert "Bob" in lines[reply_index + 1]
        assert lines[reply_index + 2] == "reply text"

    def test_reply_to_a_message_not_in_this_export_gets_the_unresolved_note(self) -> None:
        rows = [
            {
                "author": '{"name": "Bob"}',
                "create_time": 1700000000,
                "content_preview": "orphan reply",
                "metadata": None,
                "is_sys_message": 0,
                "reply_to_id": "missing",
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html)
        assert "replying to a message not included in this export" in text

    def test_a_deleted_message_shows_the_placeholder_body(self) -> None:
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "gone",
                "metadata": None,
                "is_sys_message": 0,
                "is_deleted": 1,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html)
        assert "(this message has been deleted)" in text
        assert "gone" not in text

    def test_a_system_message_is_shown_between_em_dashes_not_as_an_ordinary_message(self) -> None:
        rows = [
            {
                "author": '{"name": "System"}',
                "create_time": 1700000000,
                "content_preview": "",
                "metadata": None,
                "is_sys_message": 1,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html)
        assert "— (system event) —" in text

    def test_nested_divs_inside_an_html_body_dont_truncate_the_message(self) -> None:
        """Regression test: a real Teams "html" contentType body commonly
        uses plain, unclassed ``<div>``s for line breaks -- these must
        keep inheriting the surrounding "body" role, not be mistaken for
        that role-bearing ``<div>``'s own matching close just because they
        share a tag name."""
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "",
                "metadata": json.dumps(
                    {"body": {"contentType": "html", "content": "<div>line one</div><div>line two</div>"}}
                ),
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html)
        assert "line one" in text
        assert "line two" in text

    def test_a_sticker_shows_its_own_alt_placeholder_not_raw_base64(self) -> None:
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "",
                "metadata": json.dumps({"body": {"contentType": "html", "content": '<img src="sticker1">'}}),
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General", stickers_by_msg_id={"1": {"sticker1": "AAAA"}}).encode(
            "utf-8"
        )
        text = render_teams_chat_preview(html)
        assert "[sticker]" in text
        assert "AAAA" not in text

    def test_an_attachment_with_content_shows_its_header_and_body(self) -> None:
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "see attached",
                "metadata": json.dumps(
                    {
                        "body": {"contentType": "text", "content": "see attached"},
                        "attachments": [
                            {
                                "name": "card.json",
                                "contentType": "application/vnd.microsoft.card.adaptive",
                                "content": '{"type": "AdaptiveCard"}',
                            }
                        ],
                    }
                ),
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html)
        assert "card.json" in text
        assert '{"type": "AdaptiveCard"}' in text

    def test_an_attachment_with_no_content_shows_the_not_included_placeholder(self) -> None:
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "see attached",
                "metadata": json.dumps(
                    {
                        "body": {"contentType": "text", "content": "see attached"},
                        "attachments": [{"name": "report.pdf", "contentType": "application/pdf", "content": None}],
                    }
                ),
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html)
        assert "report.pdf" in text
        assert "content not included in this offline export" in text

    def test_an_empty_channel_shows_the_no_messages_placeholder(self) -> None:
        html = render_channel_html([], channel_name="Empty").encode("utf-8")
        assert render_teams_chat_preview(html) == "(no messages)"

    def test_a_read_window_truncated_mid_sticker_shows_a_placeholder_not_raw_base64(self) -> None:
        # A real sticker sits directly inside its own message's body, with
        # no other body text ahead of it -- the shape
        # ``_drop_trailing_unterminated_tag`` expects.
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "",
                "metadata": json.dumps({"body": {"contentType": "html", "content": '<img src="sticker1">'}}),
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        fake_base64 = "AAAA" * 20_000
        html = render_channel_html(
            rows, channel_name="General", stickers_by_msg_id={"1": {"sticker1": fake_base64}}
        ).encode("utf-8")
        truncated = html[: html.index(b"base64,") + len("base64,") + 5000]
        text = render_teams_chat_preview(truncated, max_chars=100_000)
        assert "AAAA" not in text
        assert "[image, ≥" in text
        assert "not shown in preview" in text

    def test_long_transcript_is_truncated_with_a_note(self) -> None:
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000 + i,
                "content_preview": "x" * 200,
                "metadata": None,
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": str(i),
            }
            for i in range(50)
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html, max_chars=500)
        assert len(text) < 5000
        assert "truncated" in text.lower()

    def test_truncation_drops_the_oldest_messages_keeping_the_newest(self) -> None:
        # The whole point of this renderer's own truncation: a channel's
        # newest messages are what a user wants visible, not its oldest
        # ones -- a raw character-count truncation from the tail (the
        # generic render_html_preview's own approach) would get this
        # backwards.
        rows = [
            {
                "author": f'{{"name": "User{i}"}}',
                "create_time": 1700000000 + i * 100,
                "content_preview": f"message number {i}",
                "metadata": None,
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": str(i),
            }
            for i in range(30)
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html, max_chars=500)
        assert "message number 29" in text
        assert "message number 0" not in text
        assert "earlier messages truncated" in text.lower()
        # The note comes before the kept messages, not after -- dropping
        # from the front means whatever's missing is always older
        # messages.
        assert text.index("earlier messages truncated") < text.index("message number")

    def test_truncation_note_appears_only_when_something_was_actually_dropped(self) -> None:
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "hi",
                "metadata": None,
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html, max_chars=4000)
        assert "truncated" not in text.lower()

    def test_a_single_message_longer_than_max_chars_is_still_shown_in_full(self) -> None:
        # Never cut the one message actually kept -- a long last message
        # is still more useful shown whole than truncated mid-sentence.
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "y" * 3000,
                "metadata": None,
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html, max_chars=500)
        assert "y" * 3000 in text


__all__: list[str] = []
