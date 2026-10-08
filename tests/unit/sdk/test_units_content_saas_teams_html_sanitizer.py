"""Unit tests for ``synology_apm_repo.sdk.units.content.saas_teams_html_sanitizer``:
what a Teams "html" message body may and may not carry into the exported page."""

from __future__ import annotations

import pytest

from synology_apm_repo.sdk.units.content.saas_teams_html_sanitizer import render_message_body_html


def _render(raw: str, stickers: dict[str, str] | None = None) -> str:
    return render_message_body_html(raw, stickers or {})


def test_text_without_markup_is_escaped() -> None:
    assert _render("a & b > c") == "a &amp; b &gt; c"


def test_allowed_tags_keep_their_structure_and_lose_every_attribute() -> None:
    raw = '<div style="color:red" onclick="x()"><p class="c">hi <b id="1">there</b></p><br/><hr></div>'
    assert _render(raw) == "<div><p>hi <b>there</b></p><br><hr></div>"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("<script>alert(1 < 2 && 3 > 2)</script>", "alert(1 &lt; 2 &amp;&amp; 3 &gt; 2)"),
        ("<style>a > b{display:none}</style>", "a &gt; b{display:none}"),
        ('<iframe src="https://example.com"></iframe>text', "text"),
        ('<attachment id="1"></attachment>after', "after"),
        ("<!-- <script>x</script> -->kept", "kept"),
    ],
)
def test_a_tag_outside_the_allowlist_is_dropped_and_its_text_escaped(raw: str, expected: str) -> None:
    assert _render(raw) == expected


@pytest.mark.parametrize(
    "href",
    ["javascript:alert(1)", "JAVASCRIPT:alert(1)", "data:text/html,<script>x</script>", "//example.com", ""],
)
def test_a_link_without_an_http_scheme_keeps_its_text_but_not_its_href(href: str) -> None:
    assert _render(f'<a href="{href}">click</a>') == "<a>click</a>"


def test_an_http_link_keeps_its_escaped_href_and_opens_safely() -> None:
    assert _render('<a href="https://example.com/?a=1&amp;b=&quot;2&quot;">x</a>') == (
        '<a href="https://example.com/?a=1&amp;b=&quot;2&quot;" target="_blank" rel="noopener noreferrer">x</a>'
    )


def test_a_mention_and_an_emoji_render_as_text() -> None:
    assert _render('<at id="0">Alice</at> <emoji alt="🙂"></emoji><emoji alt="&lt;b&gt;"/>') == (
        '<span class="mention">@Alice</span>🙂&lt;b&gt;'
    )


def test_an_end_tag_that_does_not_match_the_innermost_open_tag_is_ignored() -> None:
    assert _render("<b><i>x</b></i>") == "<b><i>x</i></b>"


def test_a_tag_left_open_is_closed_at_the_end_of_the_body() -> None:
    """Otherwise the browser carries the formatting into every later message."""
    assert _render("<b><a href='https://example.com'>x") == (
        '<b><a href="https://example.com" target="_blank" rel="noopener noreferrer">x</a></b>'
    )


def test_a_sticker_image_is_embedded_and_any_other_image_becomes_a_placeholder() -> None:
    stickers = {"https://example.com/s.jpg": "/9j/AAAA+/=="}
    raw = '<img src="https://example.com/s.jpg"><img src="https://example.com/other.png" alt="<chart>"><img/>'
    assert _render(raw, stickers) == (
        '<img class="sticker" alt="[sticker]" src="data:image/jpeg;base64,/9j/AAAA+/==">'
        '<span class="inline-media">🖼 &lt;chart&gt;</span>'
        '<span class="inline-media">🖼 image</span>'
    )


def test_sticker_content_that_is_not_base64_becomes_a_placeholder() -> None:
    """The sticker table is backup content: a quote in it must not end the ``src`` attribute."""
    stickers = {"https://example.com/s.jpg": 'x" onerror="alert(1)'}
    assert _render('<img src="https://example.com/s.jpg" alt="s">', stickers) == (
        '<span class="inline-media">🖼 s</span>'
    )


def test_line_wrapped_sticker_content_is_embedded_unwrapped() -> None:
    stickers = {"https://example.com/s.jpg": "/9j/AAAA\r\n+/==\n"}
    assert _render('<img src="https://example.com/s.jpg">', stickers) == (
        '<img class="sticker" alt="[sticker]" src="data:image/jpeg;base64,/9j/AAAA+/==">'
    )
