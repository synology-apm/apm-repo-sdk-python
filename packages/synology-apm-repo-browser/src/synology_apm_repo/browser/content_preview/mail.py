"""Mail (``.eml``) preview: parses via the stdlib ``email`` package."""

from __future__ import annotations

import email
import re
from email import policy
from email.message import EmailMessage, MIMEPart

from ._common import _html_bytes_to_text_with_truncation_note, _truncate

#: A real tag start (``<div``, ``<!doctype``, ``</p``), not a bare ``<``
#: (``5 < 10``, ``<3``). Unlike ``html.py``'s ``_HTML_SNIFF_RE`` it needs no
#: ``<html>``/``<!doctype>`` wrapper, since a mail body often opens with a
#: bare ``<table>``/``<div>``.
_LOOKS_LIKE_A_TAG_RE = re.compile(r"<[a-zA-Z!/]")

_NO_SUBJECT_LABEL = "(no subject)"


def render_mail_preview(data: bytes) -> str:
    """A From/To/Subject/Date header block plus a plain-text body for
    ``.eml`` bytes. From/To/Date are dropped when absent; an empty Subject
    shows ``_NO_SUBJECT_LABEL``. Parse failures propagate."""
    msg = email.message_from_bytes(data, policy=policy.default)
    header_lines = [f"{name}: {value}" for name in ("From", "To") if (value := msg.get(name))]
    header_lines.append(f"Subject: {msg.get('Subject') or _NO_SUBJECT_LABEL}")
    if date := msg.get("Date"):
        header_lines.append(f"Date: {date}")

    body_text = _mail_body_text(msg)

    text = "\n".join(header_lines)
    if body_text.strip():
        text = f"{text}\n\n{body_text}" if text else body_text
    return _truncate(text)


def _mail_body_text(msg: EmailMessage) -> str:
    """The ``text/html`` body as text (a sender's plain-text alternative is
    often poorer), falling back to ``text/plain`` when the HTML is unusable:
    an unrecognized ``charset``, a body that doesn't decode to ``str``,
    nothing tag-shaped (a size-capped read can leave the payload still
    transfer-encoded), or text that converts to blank (e.g. a tracking
    pixel)."""
    body_part = msg.get_body(preferencelist=("html", "plain"))
    if body_part is None:
        return ""
    if body_part.get_content_type() != "text/html":
        content = body_part.get_content()
        return content if isinstance(content, str) else ""
    html_text = _html_body_text_or_none(body_part)
    if html_text is not None:
        return html_text
    plain_part = msg.get_body(preferencelist=("plain",))
    if plain_part is None:
        return ""
    plain_content = plain_part.get_content()
    return plain_content if isinstance(plain_content, str) else ""


def _html_body_text_or_none(body_part: MIMEPart) -> str | None:
    """``None`` when ``body_part`` (a ``text/html`` part) is unusable, so
    ``_mail_body_text`` falls back to the plain alternative. A
    truncation-note-only result counts as unusable."""
    try:
        content = body_part.get_content()
    except LookupError:  # an unknown charset name
        return None
    if not isinstance(content, str) or not _LOOKS_LIKE_A_TAG_RE.search(content):
        return None
    text, has_real_content = _html_bytes_to_text_with_truncation_note(content.encode("utf-8"))
    return text if has_real_content else None
