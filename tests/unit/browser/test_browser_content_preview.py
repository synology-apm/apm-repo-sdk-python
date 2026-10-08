"""Pure-function tests for ``browser.content_preview``'s renderers, which the
replay Pilot tests can't cover: loading real mail/contact/calendar/Teams
content would record it into a committed fixture."""

from __future__ import annotations

import base64
import json

import pytest

from synology_apm_repo.browser.content_preview import (
    render_calendar_event_preview,
    render_contact_preview,
    render_html_preview,
    render_mail_preview,
    render_teams_chat_preview,
    visible_site_fields,
)
from synology_apm_repo.browser.content_preview._common import _MAX_PREVIEW_CHARS, _drop_trailing_unterminated_tag
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

# A sender's text/plain alternative can be a byte-for-byte copy of the raw
# HTML, so a multipart/alternative with both must prefer the text/html part.
_MULTIPART_RAW_HTML_PLAIN_EML = (
    b"From: Alice <alice@example.com>\r\n"
    b"Subject: Broken plain-text alternative\r\n"
    b'Content-Type: multipart/alternative; boundary="BOUNDARY1"\r\n'
    b"\r\n"
    b"--BOUNDARY1\r\n"
    b"Content-Type: text/plain; charset=utf-8\r\n"
    b"\r\n"
    b"<html><body><table><tr><td>Welcome, Sample User</td></tr></table></body></html>\r\n"
    b"--BOUNDARY1\r\n"
    b"Content-Type: text/html; charset=utf-8\r\n"
    b"\r\n"
    b"<html><body><p>Welcome, Sample User</p></body></html>\r\n"
    b"--BOUNDARY1--\r\n"
)

# A mail client's text/plain auto-conversion can embed a raw URL in angle
# brackets after a link's text; the text/html part never shows an href.
_MULTIPART_BRACKETED_URL_PLAIN_EML = (
    b"From: Alice <alice@example.com>\r\n"
    b"Subject: Meeting invite\r\n"
    b'Content-Type: multipart/alternative; boundary="BOUNDARY2"\r\n'
    b"\r\n"
    b"--BOUNDARY2\r\n"
    b"Content-Type: text/plain; charset=utf-8\r\n"
    b"\r\n"
    b"Join the meeting<https://example.com/join/abc123>\r\n"
    b"--BOUNDARY2\r\n"
    b"Content-Type: text/html; charset=utf-8\r\n"
    b"\r\n"
    b'<html><body><a href="https://example.com/join/abc123">Join the meeting</a></body></html>\r\n'
    b"--BOUNDARY2--\r\n"
)

# A purely decorative text/html alternative (e.g. a tracking-pixel-only
# body) next to a substantive text/plain one must not blank out the
# preview just because _html_to_text's own output is empty.
_MULTIPART_EMPTY_HTML_EML = (
    b"From: Alice <alice@example.com>\r\n"
    b"Subject: Decorative html alternative\r\n"
    b'Content-Type: multipart/alternative; boundary="BOUNDARY4"\r\n'
    b"\r\n"
    b"--BOUNDARY4\r\n"
    b"Content-Type: text/plain; charset=utf-8\r\n"
    b"\r\n"
    b"The real message content is here.\r\n"
    b"--BOUNDARY4\r\n"
    b"Content-Type: text/html; charset=utf-8\r\n"
    b"\r\n"
    b'<html><body><img src="cid:tracker"></body></html>\r\n'
    b"--BOUNDARY4--\r\n"
)

# A read window cut mid-``Content-Transfer-Encoding: base64`` stream makes
# the stdlib decode come back as the still-encoded payload (no ``<``
# anywhere). Literal bytes (fixed boundary, no generated headers) keep the
# truncation offset below deterministic.
_TRUNCATED_BASE64_HTML_BODY = base64.encodebytes(
    ("<html><body><p>Hello</p></body></html>" + "x" * 4000).encode("utf-8")
)
_TRUNCATED_BASE64_HTML_EML_FULL = (
    b"Subject: Truncated base64 html alternative\r\n"
    b'Content-Type: multipart/alternative; boundary="BOUNDARY3"\r\n'
    b"\r\n"
    b"--BOUNDARY3\r\n"
    b"Content-Type: text/plain; charset=utf-8\r\n"
    b"\r\n"
    b"Plain-text fallback content.\r\n"
    b"--BOUNDARY3\r\n"
    b"Content-Type: text/html; charset=utf-8\r\n"
    b"Content-Transfer-Encoding: base64\r\n"
    b"\r\n" + _TRUNCATED_BASE64_HTML_BODY + b"--BOUNDARY3--\r\n"
)
# Lands mid-base64-group.
_TRUNCATED_BASE64_HTML_EML = _TRUNCATED_BASE64_HTML_EML_FULL[:5558]


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
        # Unlike From/To/Date, a missing Subject is never dropped.
        text = render_mail_preview(_NO_HEADERS_EML)
        assert "Subject: (no subject)" in text

    def test_empty_subject_header_shows_the_no_subject_placeholder(self) -> None:
        text = render_mail_preview(_EMPTY_SUBJECT_EML)
        assert "Subject: (no subject)" in text

    def test_long_body_is_truncated_with_a_note(self) -> None:
        long_body = b"From: Alice <a@example.com>\r\nSubject: Long\r\n\r\n" + b"x" * 20_000
        text = render_mail_preview(long_body)
        assert len(text) < 20_000
        assert "truncated" in text.lower()
        assert "export" in text.lower()

    def test_short_content_is_not_truncated(self) -> None:
        text = render_mail_preview(_PLAIN_EML)
        assert "truncated" not in text.lower()

    def test_prefers_html_over_a_plain_alternative_that_is_actually_raw_html(self) -> None:
        text = render_mail_preview(_MULTIPART_RAW_HTML_PLAIN_EML)
        assert "Welcome, Sample User" in text
        assert "<html" not in text and "<table" not in text

    def test_prefers_html_over_a_plain_alternative_that_embeds_a_bracketed_url(self) -> None:
        text = render_mail_preview(_MULTIPART_BRACKETED_URL_PLAIN_EML)
        assert "Join the meeting" in text
        assert "https://example.com/join/abc123" not in text

    def test_a_read_window_truncated_mid_attribute_shows_a_placeholder_not_raw_base64(self) -> None:
        fake_base64 = "AAAA" * 20_000
        html_eml = (
            b"From: Alice <alice@example.com>\r\n"
            b"Subject: Long image\r\n"
            b"Content-Type: text/html; charset=utf-8\r\n"
            b"\r\n"
            b'<html><body><p>flower~~</p><img src="data:image/jpeg;base64,' + fake_base64.encode()
        )
        text = render_mail_preview(html_eml)
        assert "flower~~" in text
        assert "AAAA" not in text
        assert "[image, ≥" in text
        assert "not shown in preview" in text

    def test_falls_back_to_plain_when_the_html_alternative_flattens_to_nothing(self) -> None:
        text = render_mail_preview(_MULTIPART_EMPTY_HTML_EML)
        assert "The real message content is here." in text

    def test_falls_back_to_plain_when_a_truncated_base64_html_body_fails_to_decode(self) -> None:
        text = render_mail_preview(_TRUNCATED_BASE64_HTML_EML)
        assert "Plain-text fallback content." in text

    def test_falls_back_to_plain_when_the_html_alternatives_own_content_is_entirely_inside_a_truncated_tag(
        self,
    ) -> None:
        # The html alternative's only content is an unclosed <img>, so it
        # renders to the dropped-tag placeholder alone -- not real content.
        eml = (
            b"From: Alice <alice@example.com>\r\n"
            b"Subject: Placeholder-only html alternative\r\n"
            b'Content-Type: multipart/alternative; boundary="BOUNDARY7"\r\n'
            b"\r\n"
            b"--BOUNDARY7\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            b"\r\n"
            b"This is the real plain-text message body.\r\n"
            b"--BOUNDARY7\r\n"
            b"Content-Type: text/html; charset=utf-8\r\n"
            b"\r\n"
            b'<html><body><script>void</script><img src="data:image/jpeg;base64,'
            + b"A" * 5000
            + b"\r\n--BOUNDARY7--\r\n"
        )
        text = render_mail_preview(eml)
        assert "This is the real plain-text message body." in text

    def test_falls_back_to_plain_when_the_html_alternative_has_an_unrecognized_charset(self) -> None:
        # An unrecognized charset raises LookupError from get_content().
        eml = (
            b"From: Alice <alice@example.com>\r\n"
            b"Subject: Bad charset\r\n"
            b'Content-Type: multipart/alternative; boundary="BOUNDARY5"\r\n'
            b"\r\n"
            b"--BOUNDARY5\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            b"\r\n"
            b"Perfectly good plain text body.\r\n"
            b"--BOUNDARY5\r\n"
            b"Content-Type: text/html; charset=bogus-charset-xyz\r\n"
            b"\r\n"
            b"<html><body>hi</body></html>\r\n"
            b"--BOUNDARY5--\r\n"
        )
        text = render_mail_preview(eml)
        assert "Perfectly good plain text body." in text

    def test_a_stray_inequality_sign_is_not_mistaken_for_a_real_tag(self) -> None:
        eml = (
            b"From: Alice <alice@example.com>\r\n"
            b"Subject: Inequality signs\r\n"
            b'Content-Type: multipart/alternative; boundary="BOUNDARY6"\r\n'
            b"\r\n"
            b"--BOUNDARY6\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            b"\r\n"
            b"Plain fallback for stray inequality signs.\r\n"
            b"--BOUNDARY6\r\n"
            b"Content-Type: text/html; charset=utf-8\r\n"
            b"\r\n"
            b"5 < 10 and 20 > 15, no real markup here at all.\r\n"
            b"--BOUNDARY6--\r\n"
        )
        text = render_mail_preview(eml)
        assert "Plain fallback for stray inequality signs." in text


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
        # Built from the real render_channel_html, so the two can't drift apart.
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
        text = render_html_preview(html)
        assert text is not None
        assert len(text) < 5000
        assert "truncated" in text.lower()

    def test_html_that_flattens_to_nothing_returns_none(self) -> None:
        html = b"<!doctype html><html><head><style>.x{color:red}</style></head><body></body></html>"
        assert render_html_preview(html) is None

    def test_a_read_window_truncated_mid_attribute_shows_a_placeholder_not_raw_base64(self) -> None:
        """A large inline base64 ``<img>`` (e.g. a Teams sticker) cut off
        mid-attribute by the preview's bounded read."""
        fake_base64 = "AAAA" * 20_000
        html = (
            b'<!doctype html><html><body><div class="msg"><div class="hdr">Alice</div>'
            b'<div class="body">flower~~</div></div><div class="msg"><img class="sticker" '
            b'alt="[sticker]" src="data:image/jpeg;base64,' + fake_base64.encode()
        )
        text = render_html_preview(html)
        assert text is not None
        assert "flower~~" in text
        assert "AAAA" not in text
        assert "[image, ≥" in text
        assert "not shown in preview" in text

    def test_a_truncated_non_img_tag_gets_a_generic_placeholder_label(self) -> None:
        fake_attr = "x" * 5000
        html = b'<!doctype html><html><body><p>hi</p><div data-blob="' + fake_attr.encode()
        text = render_html_preview(html)
        assert text is not None
        assert "hi" in text
        assert "[content, ≥" in text

    def test_trailing_plain_text_after_the_last_real_tag_is_not_mistaken_for_a_truncation(self) -> None:
        html = b"<!doctype html><html><body><p>hello</p> trailing plain text with no tag at all"
        text = render_html_preview(html)
        assert text is not None
        assert "[" not in text

    def test_a_window_ending_on_a_real_tag_boundary_is_unaffected(self) -> None:
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
        # The .ics UID falls back to the event id, which must not leak into the title.
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
        # Hand-built: build_ics() always emits exactly one VEVENT.
        ics = b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n"
        assert render_calendar_event_preview(ics) == "(none)"


class TestDropTrailingUnterminatedTag:
    def test_no_greater_than_character_at_all_returns_data_unchanged(self) -> None:
        data = b"plain text with no angle brackets whatsoever"
        assert _drop_trailing_unterminated_tag(data) == (data, None)


class TestRenderContactPreview:
    def test_m365_csv_shows_full_name_and_email(self) -> None:
        meta = json.dumps(
            {
                "client_metadata": {
                    "givenName": "Alice",
                    "surname": "Wu",
                    "emailAddresses": [{"address": "alice@example.com"}],
                }
            }
        ).encode()
        csv_bytes = build_contact_csv(meta)
        text = render_contact_preview(csv_bytes)
        assert text == "Full Name: Alice Wu\nEmail: alice@example.com"

    def test_m365_csv_with_no_name_or_email_shows_the_none_placeholder(self) -> None:
        csv_bytes = build_contact_csv(json.dumps({"client_metadata": {}}).encode())
        text = render_contact_preview(csv_bytes)
        assert text == "Full Name: (none)\nEmail: (none)"

    def test_gws_json_shows_full_name_and_email(self) -> None:
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
                    "emailAddresses": [{"address": "alice@example.com"}],
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
            "Email: alice@example.com\n"
            "Job Title: Engineer\n"
            "Company: Acme\n"
            "Business Phone: 555-1000\n"
            "Home Phone: 555-2000\n"
            "Mobile Phone: 555-3000\n"
            "Address: 1 Main St, Springfield IL, 62704, US\n"
            "Notes: met at conference"
        )

    def test_gws_json_shows_every_present_optional_field(self) -> None:
        # Real shape: organizations[0].{name,title}, phoneNumbers[0].value
        # (no type), biographies[0].value, birthdays[0].text.
        data = json.dumps(
            {
                "client_metadata": {
                    "names": [{"displayName": "Alice Example"}],
                    "emailAddresses": [{"value": "alice@example.com"}],
                    "organizations": [{"name": "Example Corp", "title": "Engineer"}],
                    "phoneNumbers": [{"value": "555-0100"}],
                    "birthdays": [{"date": {"day": 2, "month": 1, "year": 1990}, "text": "1/2/1990"}],
                    "biographies": [{"value": "likes cats"}],
                }
            }
        ).encode()
        text = render_contact_preview(data)
        assert text == (
            "Full Name: Alice Example\n"
            "Email: alice@example.com\n"
            "Job Title: Engineer\n"
            "Company: Example Corp\n"
            "Phone: 555-0100\n"
            "Birthday: 1/2/1990\n"
            "Notes: likes cats"
        )

    def test_gws_phone_number_with_a_real_type_gets_a_labeled_line(self) -> None:
        data = json.dumps(
            {"client_metadata": {"phoneNumbers": [{"value": "555-1000", "formattedType": "Mobile"}]}}
        ).encode()
        text = render_contact_preview(data)
        assert "Phone (Mobile): 555-1000" in text

    @pytest.mark.parametrize(
        "address",
        [
            pytest.param(
                {"formattedValue": "1 Main St, Springfield", "streetAddress": "should not be used"},
                id="prefers_the_formatted_value_over_composing_components",
            ),
            pytest.param(
                {"streetAddress": "1 Main St", "city": "Springfield"},
                id="composes_from_components_when_no_formatted_value",
            ),
        ],
    )
    def test_gws_address(self, address: dict[str, str]) -> None:
        data = json.dumps({"client_metadata": {"addresses": [address]}}).encode()
        text = render_contact_preview(data)
        assert "Address: 1 Main St, Springfield" in text

    @pytest.mark.parametrize(
        ("date", "expected"),
        [
            pytest.param(
                {"year": 1990, "month": 1, "day": 2}, "Birthday: 1990-01-02", id="composed_from_date_when_no_text"
            ),
            pytest.param({"month": 1, "day": 2}, "Birthday: 01-02", id="without_a_year_omits_it"),
            pytest.param({"year": 1990}, "Birthday: 1990", id="year_only_shows_just_the_year"),
            # day=0 is the People API's "unspecified" sentinel: the month
            # still shows, not a fall back to year-only.
            pytest.param(
                {"year": 1990, "month": 6, "day": 0},
                "Birthday: 1990-06",
                id="a_known_month_and_unspecified_day_still_shows_the_month",
            ),
            pytest.param({"day": 15}, "Birthday: 15", id="a_known_day_and_unspecified_month_still_shows_the_day"),
        ],
    )
    def test_gws_birthday_composes_from_the_date_parts_present(self, date: dict[str, int], expected: str) -> None:
        data = json.dumps({"client_metadata": {"birthdays": [{"date": date}]}}).encode()
        text = render_contact_preview(data)
        assert expected in text

    @pytest.mark.parametrize(
        ("date", "expected", "forbidden"),
        [
            # Real GWS data for a year-only birthday: month/day are 0, not absent.
            pytest.param(
                {"year": 1990, "month": 0, "day": 0},
                "Birthday: 1990",
                "00",
                id="month_and_day_as_the_unspecified_sentinel_shows_just_the_year",
            ),
            pytest.param(
                {"year": 0, "month": 6, "day": 15},
                "Birthday: 06-15",
                "0-06-15",
                id="the_unspecified_year_sentinel_omits_the_year",
            ),
        ],
    )
    def test_gws_birthday_omits_the_zero_unspecified_sentinel(
        self, date: dict[str, int], expected: str, forbidden: str
    ) -> None:
        data = json.dumps({"client_metadata": {"birthdays": [{"date": date}]}}).encode()
        text = render_contact_preview(data)
        assert expected in text
        assert forbidden not in text

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
    """Every case builds its HTML via the real ``render_channel_html``."""

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
        # The avatar's single-letter placeholder is not rendered.
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
        """Unclassed ``<div>``s (line breaks in a Teams html body) inherit
        the enclosing body role rather than closing it."""
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
        text = render_teams_chat_preview(truncated)
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
        text = render_teams_chat_preview(html)
        assert len(text) < 5000
        assert "truncated" in text.lower()

    def test_truncation_drops_the_oldest_messages_keeping_the_newest(self) -> None:
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
            for i in range(300)
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html)
        assert "message number 299" in text
        assert "message number 0" not in text
        assert "earlier messages truncated" in text.lower()
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
        text = render_teams_chat_preview(html)
        assert "truncated" not in text.lower()

    def test_a_single_message_longer_than_the_limit_is_still_shown_in_full(self) -> None:
        rows = [
            {
                "author": '{"name": "Alice"}',
                "create_time": 1700000000,
                "content_preview": "y" * (_MAX_PREVIEW_CHARS + 1000),
                "metadata": None,
                "is_sys_message": 0,
                "reply_to_id": None,
                "msg_id": "1",
            }
        ]
        html = render_channel_html(rows, channel_name="General").encode("utf-8")
        text = render_teams_chat_preview(html)
        assert "y" * (_MAX_PREVIEW_CHARS + 1000) in text
