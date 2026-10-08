"""Unit tests for ``synology_apm_repo.sdk.units.content.saas_teams_chat``'s channel/chat
HTML rendering — pure functions over synthetic message rows."""

from __future__ import annotations

import json

import pytest

from synology_apm_repo.sdk.units.content.saas_teams_chat import (
    _REPLY_PREVIEW_LENGTH,
    _RenderedMessage,
    _reply_note_html,
    render_channel_html,
)


def _row(**kwargs: object) -> dict[str, object]:
    base: dict[str, object] = {
        "author": None,
        "create_time": None,
        "content_preview": None,
        "metadata": None,
        "is_sys_message": 0,
        "is_deleted": 0,
        "reply_to_id": None,
        "msg_id": None,
    }
    base.update(kwargs)
    return base


class TestRenderChannelHtml:
    """The pure function, independent of the provider/DB plumbing
    ``test_units_saas_teams_chat.py``'s ``TestChannelUnit`` exercises end to end."""

    def test_basic_message_shows_sender_and_content(self) -> None:
        row = _row(
            metadata=json.dumps(
                {"from": {"user": {"displayName": "Alice"}}, "body": {"content": "hello", "contentType": "text"}}
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "Alice" in page
        assert "hello" in page

    def test_author_column_is_the_fallback_sender_when_metadata_from_is_absent(self) -> None:
        row = _row(author=json.dumps({"name": "Bob"}), content_preview="hi")
        page = render_channel_html([row], channel_name="General")
        assert "Bob" in page
        assert "hi" in page

    def test_unknown_sender_when_neither_source_has_a_name(self) -> None:
        row = _row(content_preview="hi")
        page = render_channel_html([row], channel_name="General")
        assert "(unknown sender)" in page

    def test_system_message_shows_generic_label_not_the_literal_tag(self) -> None:
        row = _row(
            is_sys_message=1,
            metadata=json.dumps({"body": {"content": "<systemEventMessage/>", "contentType": "html"}}),
        )
        page = render_channel_html([row], channel_name="General")
        assert "(system event)" in page
        assert "systemEventMessage" not in page

    def test_content_preview_used_when_metadata_body_is_absent(self) -> None:
        row = _row(content_preview="fallback text")
        page = render_channel_html([row], channel_name="General")
        assert "fallback text" in page

    def test_attachment_with_inline_content_is_shown_in_full(self) -> None:
        row = _row(
            metadata=json.dumps(
                {
                    "body": {"content": "see attached", "contentType": "text"},
                    "attachments": [{"name": "poll.json", "contentType": "application/json", "content": '{"a":1}'}],
                }
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "poll.json" in page
        # Escaped like any other body text; it is untrusted source data.
        assert "&quot;a&quot;:1" in page

    def test_attachment_without_inline_content_shows_the_honest_placeholder(self) -> None:
        row = _row(
            metadata=json.dumps(
                {
                    "body": {"content": "see attached", "contentType": "text"},
                    "attachments": [{"name": "photo.jpg", "contentType": "image/jpeg", "contentUrl": "https://x/y"}],
                }
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "photo.jpg" in page
        assert "not included in this offline export" in page

    def test_reply_to_id_with_no_resolvable_parent_shows_a_generic_note(self) -> None:
        # "12345" matches no msg_id on this page; a raw id is never shown,
        # only a resolved sender/preview (sibling test below).
        row = _row(reply_to_id="12345", content_preview="a reply")
        page = render_channel_html([row], channel_name="General")
        assert "12345" not in page
        assert "not included in this export" in page

    def test_reply_to_id_with_a_resolvable_parent_shows_sender_and_preview(self) -> None:
        parent = _row(msg_id="1", content_preview="the original message", author=json.dumps({"name": "Alice"}))
        reply = _row(msg_id="2", reply_to_id="1", content_preview="a reply", author=json.dumps({"name": "Bob"}))
        page = render_channel_html([parent, reply], channel_name="General")
        assert "replying to" in page
        assert "Alice" in page
        assert "the original message" in page

    def test_messages_are_sorted_chronologically_not_by_input_order(self) -> None:
        early = _row(metadata=json.dumps({"createdDateTime": "2023-01-01T00:00:00Z", "body": {"content": "first"}}))
        late = _row(metadata=json.dumps({"createdDateTime": "2023-06-01T00:00:00Z", "body": {"content": "second"}}))
        page = render_channel_html([late, early], channel_name="General")  # deliberately out of order
        assert page.index("first") < page.index("second")

    def test_sender_is_html_escaped_even_though_body_tags_are_allowlisted(self) -> None:
        row = _row(
            metadata=json.dumps(
                {
                    "from": {"user": {"displayName": "<script>alert(1)</script>"}},
                    "body": {"content": "hi", "contentType": "text"},
                }
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "<script>alert(1)</script>" not in page
        assert "&lt;script&gt;" in page

    def test_confirmed_real_safe_tags_render_structurally_not_as_escaped_text(self) -> None:
        # Tags in saas_teams_html_sanitizer's _ALLOWED_STRUCTURAL_TAGS.
        row = _row(
            metadata=json.dumps({"body": {"content": "<div><b>bold</b> <em>emph</em></div>", "contentType": "html"}})
        )
        page = render_channel_html([row], channel_name="General")
        assert "<b>bold</b>" in page
        assert "<em>emph</em>" in page

    def test_a_tag_not_in_the_allowlist_is_dropped_but_its_text_content_survives_escaped(self) -> None:
        # A non-allowlisted tag is never emitted; HTMLParser reports
        # <script>'s inner text as CDATA, which handle_data still escapes.
        row = _row(metadata=json.dumps({"body": {"content": "<script>alert(1)</script>", "contentType": "html"}}))
        page = render_channel_html([row], channel_name="General")
        assert "<script>" not in page
        assert "alert(1)" in page  # inert text, not executable markup

    def test_channel_name_is_html_escaped_in_title_and_heading(self) -> None:
        page = render_channel_html([], channel_name="<script>x</script>")
        assert "<script>x</script>" not in page
        assert "&lt;script&gt;x&lt;/script&gt;" in page

    def test_empty_channel_produces_a_valid_page_with_no_messages(self) -> None:
        page = render_channel_html([], channel_name="Empty")
        assert page.startswith("<!doctype html>")
        assert "Empty" in page

    def test_real_sticker_img_is_rendered_as_a_real_embedded_image(self) -> None:
        """A sticker ``<img src="...">`` in an ``"html"`` body is matched to this message's
        ``sticker_info_table`` row (not ``metadata.attachments[]``) and embedded."""
        row = _row(
            msg_id="123",
            metadata=json.dumps(
                {
                    "body": {
                        "content": '<div><img src="https://graph.microsoft.com/sticker1" width="100">caption</div>',
                        "contentType": "html",
                    }
                }
            ),
        )
        stickers_by_msg_id = {"123": {"https://graph.microsoft.com/sticker1": "BASE64DATA"}}
        page = render_channel_html([row], channel_name="General", stickers_by_msg_id=stickers_by_msg_id)
        assert '<img class="sticker" alt="[sticker]" src="data:image/jpeg;base64,BASE64DATA">' in page
        assert "caption" in page
        # Only the embedded image data appears, never the source URL.
        assert "graph.microsoft.com" not in page

    def test_unmatched_img_src_is_dropped_not_shown_as_broken_markup(self) -> None:
        row = _row(
            msg_id="123",
            metadata=json.dumps(
                {"body": {"content": '<div><img src="https://not-a-real-sticker">text</div>', "contentType": "html"}}
            ),
        )
        stickers_by_msg_id = {"123": {"https://graph.microsoft.com/sticker1": "BASE64DATA"}}
        page = render_channel_html([row], channel_name="General", stickers_by_msg_id=stickers_by_msg_id)
        assert "not-a-real-sticker" not in page
        assert "text" in page

    def test_html_body_renders_structurally_even_with_no_stickers_for_this_message(self) -> None:
        """With no ``stickers_by_msg_id`` entry for this message (only
        another's), structural tags still render."""
        row = _row(
            msg_id="999",
            metadata=json.dumps({"body": {"content": "<div>plain <b>html</b></div>", "contentType": "html"}}),
        )
        stickers_by_msg_id = {"123": {"https://x/y": "b64"}}
        page = render_channel_html([row], channel_name="General", stickers_by_msg_id=stickers_by_msg_id)
        assert "<b>html</b>" in page
        assert "plain" in page

    def test_deleted_message_shows_a_placeholder_not_a_blank_body(self) -> None:
        # Real shape: is_deleted=1 rows have metadata.body.content emptied to "".
        row = _row(
            is_deleted=1,
            content_preview="<The message is deleted>",
            metadata=json.dumps({"body": {"content": "", "contentType": "text"}}),
        )
        page = render_channel_html([row], channel_name="General")
        assert "(this message has been deleted)" in page
        assert "<The message is deleted>" not in page  # our own label, not the raw connector placeholder verbatim

    def test_deleted_message_omits_attachments(self) -> None:
        row = _row(
            is_deleted=1,
            metadata=json.dumps(
                {
                    "body": {"content": "", "contentType": "text"},
                    "attachments": [{"name": "should-not-appear.txt", "contentType": "text/plain", "content": "x"}],
                }
            ),
        )
        page = render_channel_html([row], channel_name="General")
        assert "should-not-appear.txt" not in page

    @pytest.mark.parametrize(
        "content",
        [
            # Real shape: Teams inserts <emoji id="..." alt="🙂" title="">,
            # never a bare Unicode character.
            pytest.param('<emoji id="smile" alt="🙂" title=""></emoji>hi', id="emoji_tag"),
            pytest.param('<emoji id="smile" alt="🙂" />hi', id="self_closed_emoji_tag"),
        ],
    )
    def test_emoji_tag_renders_its_own_alt_character(self, content: str) -> None:
        row = _row(metadata=json.dumps({"body": {"content": content, "contentType": "html"}}))
        page = render_channel_html([row], channel_name="General")
        assert "🙂" in page
        assert "hi" in page

    def test_self_closed_img_tag_is_handled_the_same_way_as_an_unclosed_one(self) -> None:
        # HTMLParser calls handle_startendtag() only for a literal "/>";
        # the other <img> tests use the unclosed shape.
        row = _row(
            metadata=json.dumps(
                {
                    "body": {
                        "content": '<img alt="Self-closed" src="https://graph.microsoft.com/v1.0/x" />',
                        "contentType": "html",
                    }
                }
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "Self-closed" in page

    def test_self_closed_void_structural_tag_is_preserved(self) -> None:
        row = _row(metadata=json.dumps({"body": {"content": "line one<br/>line two", "contentType": "html"}}))
        page = render_channel_html([row], channel_name="General")
        assert "<br>" in page

    def test_at_tag_renders_as_a_styled_mention_not_a_raw_id(self) -> None:
        row = _row(
            metadata=json.dumps({"body": {"content": 'hi <at id="0">Alice Example</at>!', "contentType": "html"}})
        )
        page = render_channel_html([row], channel_name="General")
        assert '<span class="mention">@Alice Example</span>' in page
        assert 'id="0"' not in page

    def test_unresolved_inline_image_shows_its_alt_text_instead_of_vanishing(self) -> None:
        # Real shape: an inline <img> whose src is an offline-unfetchable
        # Graph URL; the alt text is shown instead of dropping it silently.
        row = _row(
            metadata=json.dumps(
                {
                    "body": {
                        "content": '<img alt="Team outing photo" src="https://graph.microsoft.com/v1.0/x">',
                        "contentType": "html",
                    }
                }
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert "Team outing photo" in page
        assert "graph.microsoft.com" not in page

    def test_link_with_safe_scheme_keeps_a_real_href(self) -> None:
        row = _row(
            metadata=json.dumps(
                {"body": {"content": '<a href="https://example.com/page">click</a>', "contentType": "html"}}
            )
        )
        page = render_channel_html([row], channel_name="General")
        assert '<a href="https://example.com/page"' in page
        assert "click" in page

    def test_link_with_unsafe_scheme_drops_the_href_but_keeps_the_text(self) -> None:
        row = _row(
            metadata=json.dumps({"body": {"content": '<a href="javascript:alert(1)">click</a>', "contentType": "html"}})
        )
        page = render_channel_html([row], channel_name="General")
        assert "javascript:" not in page
        assert "click" in page

    def test_void_structural_tags_render_with_no_matching_close_tag(self) -> None:
        content = "line one<br>line two<hr>"
        row = _row(metadata=json.dumps({"body": {"content": content, "contentType": "html"}}))
        page = render_channel_html([row], channel_name="General")
        assert "<br>" in page
        assert "<hr>" in page
        assert "</br>" not in page and "</hr>" not in page

    def test_confirmed_real_structural_tags_render_headings_lists_and_tables(self) -> None:
        # Real shape: headings, lists and a table in one message.
        content = "<h1>Title</h1><ul><li>one</li><li>two</li></ul><table><tr><td>a</td><td>b</td></tr></table>"
        row = _row(metadata=json.dumps({"body": {"content": content, "contentType": "html"}}))
        page = render_channel_html([row], channel_name="General")
        assert "<h1>Title</h1>" in page
        assert "<li>one</li>" in page
        assert "<td>a</td>" in page

    def test_date_separator_appears_once_per_day_not_once_per_message(self) -> None:
        same_day_1 = _row(metadata=json.dumps({"createdDateTime": "2023-01-01T01:00:00Z", "body": {"content": "a"}}))
        same_day_2 = _row(metadata=json.dumps({"createdDateTime": "2023-01-01T02:00:00Z", "body": {"content": "b"}}))
        next_day = _row(metadata=json.dumps({"createdDateTime": "2023-01-02T01:00:00Z", "body": {"content": "c"}}))
        page = render_channel_html([same_day_1, same_day_2, next_day], channel_name="General")
        assert page.count('class="date-sep"') == 2
        assert "2023-01-01" in page
        assert "2023-01-02" in page


def _rendered_message(
    *,
    msg_id: str | None = "m1",
    reply_to_id: str | None = None,
    is_deleted: bool = False,
    is_system: bool = False,
    sender: str = "Alice",
    content: str = "hello",
    preview: str = "hello",
) -> _RenderedMessage:
    return _RenderedMessage(
        created=None,
        sender=sender,
        is_system=is_system,
        is_deleted=is_deleted,
        msg_id=msg_id,
        reply_to_id=reply_to_id,
        content=content,
        preview=preview,
        attachments=(),
    )


class TestReplyNoteHtml:
    def test_no_reply_to_id_is_empty(self) -> None:
        message = _rendered_message(reply_to_id=None)
        assert _reply_note_html(message, {}) == ""

    def test_parent_not_in_this_export_gets_the_unresolved_note(self) -> None:
        message = _rendered_message(reply_to_id="missing-parent")
        assert "not included in this export" in _reply_note_html(message, {})

    def test_deleted_parent_shows_the_deleted_placeholder(self) -> None:
        parent = _rendered_message(msg_id="p1", is_deleted=True, sender="Bob")
        message = _rendered_message(reply_to_id="p1")
        html = _reply_note_html(message, {"p1": parent})
        assert "Bob" in html
        assert "deleted" in html.lower()

    def test_system_parent_shows_its_own_content_not_the_preview(self) -> None:
        parent = _rendered_message(msg_id="p1", is_system=True, sender="System", content="Alice joined the chat")
        message = _rendered_message(reply_to_id="p1")
        html = _reply_note_html(message, {"p1": parent})
        assert "Alice joined the chat" in html

    def test_parent_with_no_preview_falls_back_to_ellipsis(self) -> None:
        parent = _rendered_message(msg_id="p1", sender="Bob", preview="")
        message = _rendered_message(reply_to_id="p1")
        html = _reply_note_html(message, {"p1": parent})
        assert "…" in html

    def test_parent_with_a_real_preview_gets_truncated_into_the_snippet(self) -> None:
        preview = "abcdefghij" * ((_REPLY_PREVIEW_LENGTH // 10) + 2)
        parent = _rendered_message(msg_id="p1", sender="Bob", preview=preview)
        message = _rendered_message(reply_to_id="p1")
        html = _reply_note_html(message, {"p1": parent})
        assert f": {preview[: _REPLY_PREVIEW_LENGTH - 1]}…</div>" in html
        assert preview[:_REPLY_PREVIEW_LENGTH] not in html
