"""Tests for the callback registry and name conversion."""

from typing import cast

import polars as pl

from open_icu.callbacks.proto import CallbackProtocol
from open_icu.callbacks.registry import register_callback_cls, registry
from open_icu.utils.name import camel_to_snake


def test_builtins_are_registered_under_snake_case() -> None:
    for name in ["col", "const", "add_offset", "to_datetime", "first_not_null", "split_explode", "greater_than"]:
        assert name in registry


def test_camel_to_snake() -> None:
    assert camel_to_snake("AddOffset") == "add_offset"
    assert camel_to_snake("Col") == "col"
    assert camel_to_snake("FirstNotNull") == "first_not_null"


def test_register_custom_callback() -> None:
    @register_callback_cls
    class MyTestOnlyCallback:
        def __init__(self, value: object, output: str | None = None) -> None:
            self.value = value
            self.output = output

        def __call__(self, lf: pl.LazyFrame) -> pl.Expr:
            return pl.lit(self.value)

    try:
        assert "my_test_only_callback" in registry
        # The registry stores callback *classes*; instantiate like the interpreter does.
        cls = cast("type[CallbackProtocol]", registry.get("my_test_only_callback"))
        assert cls is not None
        lf = pl.LazyFrame({"a": [1]})
        df = lf.with_columns(cls(42)(lf).alias("out")).collect()
        assert isinstance(df, pl.DataFrame)
        assert df["out"].to_list() == [42]
    finally:
        registry.unregister("my_test_only_callback")


def test_register_does_not_overwrite_by_default() -> None:
    original = registry.get("col")
    registry.register("col", object())  # ty: ignore[invalid-argument-type]
    assert registry.get("col") is original
