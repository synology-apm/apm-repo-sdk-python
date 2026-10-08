"""Shared modal-dialog CSS builder."""

from __future__ import annotations


def modal_box_css(name: str, *, width: int, guard_child_horizontal: bool = False) -> str:
    """The centered-dialog CSS shared by the package's ``ModalScreen`` subclasses.

    Centers the screen and sizes its direct-child ``Vertical`` to ``width``
    cells with the common border/background/padding; concatenate it with the
    screen's own ``DEFAULT_CSS`` rules.

    ``guard_child_horizontal`` pins a direct-child ``Horizontal`` to
    ``height: auto``. Without it the ``Horizontal``'s default ``1fr`` resolves
    against the ``Screen`` and stretches the ``height: auto`` dialog box to
    the full terminal.
    """
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
