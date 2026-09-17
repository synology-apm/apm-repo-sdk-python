"""Silencing dependency logging before it reaches the CLI/TUI's own output —
shared by both surfaces since they must behave identically here: a
per-surface copy would let a future fix (encoding, handler behavior, ...)
land in only one of them.

With no handler configured anywhere, ``logging`` falls back to its
last-resort handler, which writes every WARNING and above straight to
stderr — ``smbprotocol`` alone emits one per pooled connection each time a
session is torn down. A ``NullHandler`` on the root logger is what stops
that: ``Logger.callHandlers`` only reaches the last-resort path when it
finds no handler at all.
"""

from __future__ import annotations

import logging
import os

LOG_FILE_ENV = "SYNOLOGY_APM_REPO_LOG"


def configure_logging() -> None:
    """Adds a ``NullHandler`` to the root logger, or redirects everything to
    a file named by ``LOG_FILE_ENV`` instead, for when a backend actually
    needs debugging."""
    log_file = os.environ.get(LOG_FILE_ENV, "")
    if log_file:
        logging.basicConfig(
            filename=log_file,
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            # Without these the handler writes in the machine's locale
            # encoding (cp950 on a zh-TW Windows box), so a log naming a
            # non-ASCII share or path comes back as mojibake to whoever is
            # meant to read it.
            encoding="utf-8",
            errors="backslashreplace",
        )
        return
    logging.getLogger().addHandler(logging.NullHandler())
