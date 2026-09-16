"""``safe()`` — escape arbitrary text before it reaches a Rich-markup-parsing
call, shared by the CLI's ``rich.console.Console.print()`` calls and the
TUI's ``Static.update()``/``Static(...)`` calls (``markup=True`` is the
default for both). Any genuinely dynamic text reaching one of those calls
(an exception's ``str()``, a real backup-derived display name, a
user-typed path) risks breaking whichever entry point receives it
unescaped: the TUI's ``Static.update()`` crashes outright with
``MarkupError``, while the CLI's ``Console.print()`` sometimes instead
silently swallows a bracketed suffix as an unclosed style span.

``DataTable``/``Tree`` labels don't need this — those widgets render a
plain ``str`` as literal ``Text`` rather than re-parsing it as markup.
"""

from __future__ import annotations

from rich.markup import escape


def safe(value: object) -> str:
    """``str(value)`` with Rich markup characters escaped, so it can be
    interpolated into an f-string alongside literal ``[color]`` tags (or
    passed alone) without risking a markup-parse failure — the literal
    tags written by the calling code are untouched since ``escape()``
    only affects the *argument*, applied before the f-string is built."""
    return escape(str(value))
