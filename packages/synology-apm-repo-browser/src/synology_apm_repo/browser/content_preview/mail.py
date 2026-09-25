"""Mail (``.eml``) preview: parses via the stdlib ``email`` package."""

from __future__ import annotations

import email
from email import policy

from ._common import _html_to_text, _truncate

_NO_SUBJECT_LABEL = "(no subject)"


def render_mail_preview(data: bytes, *, max_chars: int = 4000) -> str:
    """Parse ``.eml`` bytes (RFC822/MIME — the shape
    ``units/content/saas_mail.py::build_eml`` always produces) into a
    From/To/Subject/Date header block plus a plain-text body — the
    ``text/html`` body case reuses ``_html_to_text`` rather than showing raw
    markup. From/To/Date are dropped when absent; Subject is never dropped
    — an empty/missing one shows ``_NO_SUBJECT_LABEL`` instead, the same
    always-shown-with-a-placeholder treatment ``render_calendar_event_preview``
    gives Title. Genuine parse failures propagate — deliberately not
    wrapped in a try/except here, since ``UnitScreen._load_preview`` is the
    one caller that catches a preview failure."""
    msg = email.message_from_bytes(data, policy=policy.default)
    header_lines = [f"{name}: {value}" for name in ("From", "To") if (value := msg.get(name))]
    header_lines.append(f"Subject: {msg.get('Subject') or _NO_SUBJECT_LABEL}")
    if date := msg.get("Date"):
        header_lines.append(f"Date: {date}")

    body_text = ""
    body_part = msg.get_body(preferencelist=("plain", "html"))
    if body_part is not None:
        content = body_part.get_content()
        if isinstance(content, str):
            body_text = _html_to_text(content) if body_part.get_content_type() == "text/html" else content

    text = "\n".join(header_lines)
    if body_text.strip():
        text = f"{text}\n\n{body_text}" if text else body_text
    return _truncate(text, max_chars)
