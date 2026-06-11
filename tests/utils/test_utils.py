"""Tests for utility helpers (importer, name conversion, generic type introspection)."""

from pathlib import Path

import pytest

from open_icu.utils.importer import import_callable
from open_icu.utils.type import get_generic_type


class TestImportCallable:
    def test_imports_by_dotted_path(self) -> None:
        assert import_callable("pathlib.Path") is Path

    def test_missing_module_raises(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            import_callable("no_such_module.Thing")

    def test_missing_attribute_raises(self) -> None:
        with pytest.raises(AttributeError):
            import_callable("pathlib.NoSuchThing")


class TestGetGenericType:
    def test_resolves_direct_generic_base(self) -> None:
        class Base[T]:
            pass

        class Concrete(Base[int]):
            pass

        assert get_generic_type(Concrete) is int

    def test_resolves_through_mro(self) -> None:
        class Base[T]:
            pass

        class Middle(Base[str]):
            pass

        class Leaf(Middle):
            pass

        assert get_generic_type(Leaf) is str

    def test_unparameterized_class_raises(self) -> None:
        class Plain:
            pass

        with pytest.raises(TypeError, match="Could not resolve generic type"):
            get_generic_type(Plain)
