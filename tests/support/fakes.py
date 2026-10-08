"""``@faithful_to(Real)``: a hand-written fake keeps the real API's method
signatures, checked when the fake class is defined.

A fake the code under test runs against would otherwise keep passing after
the real method grew or lost a parameter. Every public method the fake
itself defines must exist on ``Real`` (or on one of several real classes)
with the same parameters (names, kinds, which have defaults) and the same
sync/async-ness; members a fake adds beyond the real API (counters,
switches, gates) are underscore-prefixed or plain attributes. A subclass of
a decorated fake is checked against the same real class when it is defined.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, TypeVar

_T = TypeVar("_T", bound=type)


def _parameters(function: Callable[..., object]) -> list[inspect.Parameter]:
    return [p for name, p in inspect.signature(function).parameters.items() if name != "self"]


def _compatible(fake: list[inspect.Parameter], real: list[inspect.Parameter]) -> bool:
    """Same names, kinds and has-a-default, except that a real positional-only
    parameter is matched by any positional one (callers can't name it)."""
    if len(fake) != len(real):
        return False
    for f, r in zip(fake, real, strict=True):
        if (f.default is inspect.Parameter.empty) != (r.default is inspect.Parameter.empty):
            return False
        if r.kind is inspect.Parameter.POSITIONAL_ONLY:
            if f.kind not in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                return False
        elif (f.name, f.kind) != (r.name, r.kind):
            return False
    return True


def _describe(parameters: list[inspect.Parameter]) -> str:
    return "(" + ", ".join(str(p.replace(annotation=inspect.Parameter.empty)) for p in parameters) + ")"


def fidelity_problems(fake: type, *reals: type) -> list[str]:
    """Every way the methods ``fake`` itself defines differ from ``reals``':
    each must exist on one of them, and match the first that defines it."""
    problems: list[str] = []
    names = " or ".join(real.__name__ for real in reals)
    for name, member in vars(fake).items():
        if name.startswith("_") or not inspect.isfunction(member):
            continue
        real = next((r for r in reals if callable(getattr(r, name, None))), None)
        if real is None:
            problems.append(f"{fake.__name__}.{name}() does not exist on {names}")
            continue
        real_member = getattr(real, name)
        if inspect.iscoroutinefunction(member) != inspect.iscoroutinefunction(real_member):
            problems.append(f"{fake.__name__}.{name}() is sync/async unlike {real.__name__}.{name}()")
        elif not _compatible(_parameters(member), _parameters(real_member)):
            problems.append(
                f"{fake.__name__}.{name}{_describe(_parameters(member))} != "
                f"{real.__name__}.{name}{_describe(_parameters(real_member))}"
            )
    return problems


def faithful_to(*reals: type) -> Callable[[_T], _T]:
    """Class decorator: raises ``TypeError`` at definition time unless the
    decorated fake, and every later subclass of it, matches ``reals`` -- one
    class, or several for a fake standing in for an object that implements
    more than one (a provider that is also ``SupportsDirectRefLookup``)."""

    def check(fake: type) -> None:
        if problems := fidelity_problems(fake, *reals):
            raise TypeError("\n".join(problems))

    def decorate(fake: _T) -> _T:
        check(fake)
        base: type = fake
        own_hook = fake.__dict__.get("__init_subclass__")  # the fake's own, kept in the chain

        def __init_subclass__(cls: type, /, **kwargs: Any) -> None:
            if own_hook is not None:
                own_hook.__get__(None, cls)(**kwargs)
            else:
                super(base, cls).__init_subclass__(**kwargs)  # type: ignore[arg-type]  # mypy misreads a closure-held class
            check(cls)

        setattr(fake, "__init_subclass__", classmethod(__init_subclass__))  # noqa: B010 -- mypy can't type a dunder assignment
        return fake

    return decorate


def unchecked_fake(stands_in_for: str) -> Callable[[_T], _T]:
    """Class decorator for a fake whose real counterpart ``faithful_to``
    can't check: a third-party API with no importable class to compare
    against (a ``boto3`` client, a ``dissect`` entry), or an ``examples/``
    seam loaded only at test time. ``stands_in_for`` names it; the decorator
    changes nothing."""

    def decorate(fake: _T) -> _T:
        return fake

    return decorate
