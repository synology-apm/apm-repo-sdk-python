"""Shared modal-dialog CSS builder."""

from __future__ import annotations


def modal_box_css(name: str, *, width: int, guard_child_horizontal: bool = False) -> str:
    """The shared "centered dialog box" CSS every ``ModalScreen``
    subclass in this package needs (``KeyDialog``/``ConnectDialog``/
    ``ExportScreen``) — ``align: center middle`` on the screen itself,
    plus its direct-child ``Vertical`` sized to ``width`` with the same
    border/background/padding. Meant to be concatenated with each
    screen's own remaining, genuinely screen-specific ``DEFAULT_CSS``
    rules (widget ids, tab visibility, ...), not used standalone — these
    three screens' CSS was never fully identical, only this shared
    prefix was.

    ``guard_child_horizontal`` adds the same ``> Vertical > Horizontal
    { height: auto; }`` override two of the three screens need: Textual's
    own ``Horizontal``/``Vertical`` containers default to
    ``height: 1fr`` (fill remaining space) — harmless inside a screen
    that already fills the terminal, but fatal inside this
    ``height: auto`` dialog box, since with nothing pinning a
    direct-child ``Horizontal`` down, it resolves its ``1fr`` against
    the ``Screen`` itself and silently eats *all* of the dialog's
    remaining height, stretching the whole box to fill the terminal."""
    guard = (
        f"""
    {name} > Vertical > Horizontal {{
        height: auto;
    }}
    """
        if guard_child_horizontal
        else ""
    )
    return f"""
    {name} {{
        align: center middle;
    }}

    {name} > Vertical {{
        width: {width};
        height: auto;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }}
    {guard}"""
