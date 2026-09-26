"""Content Layer — Mail's ``.eml`` reassembly. Pure ``bytes -> bytes``
byte-splicing (FORMAT-SPEC.md: saas-addressing's ``X-ABL-ID`` engine); the tree-navigation
and object-fetching logic that calls this lives in ``units/saas/mail.py``.
"""

from __future__ import annotations

import base64
import quopri
from email import message_from_bytes, policy
from email.message import Message


def _decode_content_transfer_encoding(part: Message, raw: bytes) -> None:
    """Set ``part``'s payload to ``raw``, re-encoded to match whatever
    ``Content-Transfer-Encoding`` the skeleton already declared for this
    part (the skeleton keeps the real header — it only cleared the body,
    FORMAT-SPEC.md: saas-addressing) so the result stays a syntactically valid MIME
    document and ``part.get_payload(decode=True)`` recovers ``raw``
    exactly."""
    cte = (part.get("Content-Transfer-Encoding") or "").strip().lower()
    if cte == "base64":
        part.set_payload(base64.encodebytes(raw).decode("ascii"))
    elif cte == "quoted-printable":
        part.set_payload(quopri.encodestring(raw).decode("ascii"))
    else:
        # 7bit/8bit/binary (or unspecified, defaulting to 7bit semantics) —
        # the bytes are used as-is; email.generator.BytesGenerator (used by
        # Message.as_bytes()) writes a bytes payload verbatim.
        part.set_payload(raw)


def build_eml(skel_bytes: bytes, fragments_by_id: dict[str, bytes]) -> bytes:
    """Reassembles one ``.eml`` from a skeleton's raw bytes and its
    fragments, keyed by ``fragment_id`` (FORMAT-SPEC.md: saas-addressing's
    ``X-ABL-ID`` engine; matched by header **value**, never order). A part
    with no ``X-ABL-ID`` is left untouched; an unmatched fragment is simply
    unused — the skeleton is the sole authority on which parts were ever
    extracted. A ``message/rfc822`` attachment is one opaque fragment,
    never recursively re-expanded; verify its spliced bytes via
    ``part.get_payload()[0].as_bytes()``, not ``get_payload(decode=True)``
    (``None`` there — the ``email`` stdlib always re-nests such a body
    into a sub-``Message`` on parse)."""
    msg = message_from_bytes(skel_bytes, policy=policy.compat32)
    for part in msg.walk():
        abl_id = part.get("X-ABL-ID")
        if abl_id is None:
            continue
        fragment = fragments_by_id.get(abl_id)
        if fragment is None:  # pragma: no cover - defensive: a real skeleton's markers always resolve
            continue
        _decode_content_transfer_encoding(part, fragment)
        del part["X-ABL-ID"]
    result: bytes = msg.as_bytes()
    return result
