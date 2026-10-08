"""``support.fakes.faithful_to``: what it accepts and what it rejects."""

from __future__ import annotations

import pytest

from support.fakes import faithful_to, fidelity_problems


class _Real:
    async def fetch(self, key: str, *, include_deleted: bool = False) -> list[str]:
        return []

    def flush(self, *names: str) -> None: ...

    async def read(self, offset: int, length: int, /) -> bytes:
        return b""

    def _internal(self) -> None: ...


def test_a_fake_with_the_same_methods_passes() -> None:
    @faithful_to(_Real)
    class Fake:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch(self, key: str, *, include_deleted: bool = False) -> list[str]:
            return []

        def flush(self, *names: str) -> None: ...

        def _helper(self) -> None: ...  # underscore members are the fake's own business

    assert fidelity_problems(Fake, _Real) == []
    assert Fake().calls == 0  # the decorator returns the class itself


def test_a_fake_may_define_fewer_methods_than_the_real_class() -> None:
    @faithful_to(_Real)
    class Fake:
        async def fetch(self, key: str, *, include_deleted: bool = False) -> list[str]:
            return []

    assert fidelity_problems(Fake, _Real) == []


def test_a_real_positional_only_parameter_matches_any_positional_name() -> None:
    @faithful_to(_Real)
    class Fake:
        async def read(self, start: int, size: int) -> bytes:
            return b""

    assert fidelity_problems(Fake, _Real) == []


class _OtherReal:
    async def lookup(self, key: str) -> str | None:
        return None


def test_a_fake_of_several_real_classes_may_define_methods_from_each() -> None:
    @faithful_to(_Real, _OtherReal)
    class Fake:
        async def fetch(self, key: str, *, include_deleted: bool = False) -> list[str]:
            return []

        async def lookup(self, key: str) -> str | None:
            return None


def test_a_fake_of_several_real_classes_rejects_a_method_none_of_them_has() -> None:
    with pytest.raises(TypeError, match=r"Fake\.missing\(\) does not exist on _Real or _OtherReal"):

        @faithful_to(_Real, _OtherReal)
        class Fake:
            def missing(self) -> None: ...


@pytest.mark.parametrize(
    ("body", "complaint"),
    [
        ("async def fetch(self, key): return []", "Fake.fetch"),  # lost the keyword-only parameter
        ("async def fetch(self, key, *, include_deleted=False, extra=1): return []", "Fake.fetch"),
        ("async def fetch(self, key, **kwargs): return []", "Fake.fetch"),  # swallows what the real one names
        ("async def fetch(self, key, *, include_deleted): return []", "Fake.fetch"),  # lost the default
        ("async def fetch(self, name, *, include_deleted=False): return []", "Fake.fetch"),  # renamed
        ("def fetch(self, key, *, include_deleted=False): return []", "is sync/async unlike"),
        ("def flush(self): ...", "Fake.flush"),  # lost *names
        ("async def read(self, offset, length, extra): return b''", "Fake.read"),
        ("async def read(self, *, offset, length): return b''", "Fake.read"),  # keyword-only can't take positionals
        ("def invented(self): ...", "does not exist on _Real"),
    ],
)
def test_a_fake_that_drifted_from_the_real_class_fails_at_definition(body: str, complaint: str) -> None:
    namespace: dict[str, object] = {"faithful_to": faithful_to, "_Real": _Real}
    with pytest.raises(TypeError, match=complaint):
        exec(f"@faithful_to(_Real)\nclass Fake:\n    {body}\n", namespace)


def test_a_subclass_of_a_decorated_fake_is_checked_too() -> None:
    @faithful_to(_Real)
    class Fake:
        async def fetch(self, key: str, *, include_deleted: bool = False) -> list[str]:
            return []

    class Narrower(Fake):
        def flush(self, *names: str) -> None: ...

    assert issubclass(Narrower, Fake)
    with pytest.raises(TypeError, match=r"Drifted\.fetch"):

        class Drifted(Fake):
            async def fetch(self, key: str) -> list[str]:  # type: ignore[override]
                return []


def test_a_fakes_own_init_subclass_still_runs() -> None:
    seen: list[str] = []

    @faithful_to(_Real)
    class Fake:
        def __init_subclass__(cls, **kwargs: object) -> None:
            super().__init_subclass__(**kwargs)
            seen.append(cls.__name__)

    class Child(Fake):
        pass

    assert seen == ["Child"]
