"""Mail (``.eml``) preview: parses via the stdlib ``email`` package."""

from __future__ import annotations

import email
import re
from email import policy
from email.message import EmailMessage, MIMEPart

from ._common import _html_bytes_to_text_with_truncation_note, _truncate

#: A real opening tag start (``<div``, ``<!doctype``, ``</p``), not a bare
#: ``<`` — rules out a stray inequality sign or emoticon (``5 < 10``,
#: ``<3``) surviving a partial decode without requiring a full document
#: wrapper the way ``html.py``'s own ``_HTML_SNIFF_RE`` does: a real HTML
#: mail body routinely opens directly with a bare ``<table>``/``<div>``,
#: no ``<html>``/``<!doctype>`` wrapper at all.
_LOOKS_LIKE_A_TAG_RE = re.compile(r"<[a-zA-Z!/]")

_NO_SUBJECT_LABEL = "(no subject)"


def render_mail_preview(data: bytes, *, max_chars: int = 4000) -> str:
    """Parse ``.eml`` bytes (RFC822/MIME — the shape
    ``units/content/saas_mail.py::build_eml`` always produces) into a
    From/To/Subject/Date header block plus a plain-text body (see
    ``_mail_body_text``). From/To/Date are dropped when absent; Subject is
    never dropped — an empty/missing one shows ``_NO_SUBJECT_LABEL``
    instead, the same always-shown-with-a-placeholder treatment
    ``render_calendar_event_preview`` gives Title. Genuine parse failures
    propagate — deliberately not wrapped in a try/except here, since
    ``UnitScreen._load_preview`` is the one caller that catches a preview
    failure."""
    msg = email.message_from_bytes(data, policy=policy.default)
    header_lines = [f"{name}: {value}" for name in ("From", "To") if (value := msg.get(name))]
    header_lines.append(f"Subject: {msg.get('Subject') or _NO_SUBJECT_LABEL}")
    if date := msg.get("Date"):
        header_lines.append(f"Date: {date}")

    body_text = _mail_body_text(msg)

    text = "\n".join(header_lines)
    if body_text.strip():
        text = f"{text}\n\n{body_text}" if text else body_text
    return _truncate(text, max_chars)


def _mail_body_text(msg: EmailMessage) -> str:
    """Prefers the ``text/html`` alternative over ``text/plain`` when a
    message has both: a sender's own plain-text alternative is shown
    completely verbatim with no processing, so it's only the right choice
    when there's no usable HTML alternative to convert instead — whenever
    both exist, ``_html_to_text``'s own tag-stripping (which never emits a
    link's ``href``) reliably beats whatever plain-text conversion the
    original sender's mail client produced.

    Falls back to ``text/plain`` whenever the chosen ``text/html`` part
    turns out unusable — an unrecognized ``charset``, a body that doesn't
    decode to ``str``, one that doesn't contain anything tag-shaped (a
    ``Content-Transfer-Encoding`` a size-capped read window cuts mid-stream
    can come back from ``get_content()`` as its own still-encoded payload
    verbatim rather than real HTML), or one that converts to blank text
    (a purely decorative alternative, e.g. a tracking pixel)."""
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
    """``None`` whenever ``body_part`` (already known ``text/html``) turns
    out unusable, so ``_mail_body_text`` knows to fall back to the plain
    alternative instead. A truncation-note-only result (all of the real
    content sat inside the one tag a truncated read window cut) counts as
    unusable too — showing just the note while silently dropping a
    substantive plain alternative would be worse than showing that
    alternative instead."""
    try:
        content = body_part.get_content()
    except LookupError:  # an unrecognized charset name -- codecs.lookup's
        # own un-subclassed exception for this case; nothing narrower exists.
        return None
    if not isinstance(content, str) or not _LOOKS_LIKE_A_TAG_RE.search(content):
        return None
    text, has_real_content = _html_bytes_to_text_with_truncation_note(content.encode("utf-8"))
    return text if has_real_content else None
