"""Golden input/output tests for the built-in callback classes."""

from datetime import datetime

import polars as pl
import pytest

from open_icu.callbacks._callbacks.algebra import (
    Add,
    Divide,
    FloorDivide,
    Modulo,
    Multiply,
    Pow,
    Product,
    Root,
    Subtract,
    Sum,
)
from open_icu.callbacks._callbacks.conditional import Replace
from open_icu.callbacks._callbacks.filter import DropIf, DropNa, FirstDistinct
from open_icu.callbacks._callbacks.reshape import SplitExplode
from open_icu.callbacks._callbacks.selector import FirstNotNull, Max
from open_icu.callbacks._callbacks.shortcuts import Col, Const
from open_icu.callbacks._callbacks.time import AddOffset, SetTime, ToDatetime
from open_icu.callbacks._callbacks.type import Cast
from open_icu.callbacks.proto import CallbackProtocol


@pytest.fixture
def lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "a": [2.0, 4.0, 9.0],
            "b": [1.0, 2.0, 3.0],
            "maybe": [None, 5.0, None],
            "text": ["1", "2", "oops"],
        }
    )


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    df = lf.collect()
    assert isinstance(df, pl.DataFrame)
    return df


def apply(lf: pl.LazyFrame, callback: CallbackProtocol, name: str = "out") -> list:
    return collect(lf.with_columns(callback(lf).alias(name)))[name].to_list()


class TestAlgebra:
    def test_add(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Add("a", "b")) == [3.0, 6.0, 12.0]

    def test_subtract(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Subtract("a", "b")) == [1.0, 2.0, 6.0]

    def test_multiply(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Multiply("a", "b")) == [2.0, 8.0, 27.0]

    def test_divide(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Divide("a", "b")) == [2.0, 2.0, 3.0]

    def test_floor_divide(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, FloorDivide("a", "b")) == [2.0, 2.0, 3.0]

    def test_modulo(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Modulo("a", "b")) == [0.0, 0.0, 0.0]

    def test_pow(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Pow("b", 2)) == [1.0, 4.0, 9.0]

    def test_root(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Root("a", 2)) == pytest.approx([2.0**0.5, 2.0, 3.0])

    def test_root_preserves_sign(self) -> None:
        lf = pl.LazyFrame({"x": [-8.0]})
        assert apply(lf, Root("x", 3)) == pytest.approx([-2.0])

    def test_sum_variadic_and_list(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Sum("a", "b", 1)) == [4.0, 7.0, 13.0]
        assert apply(lf, Sum(["a", "b"])) == [3.0, 6.0, 12.0]

    def test_sum_empty_is_zero(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Sum()) == [0, 0, 0]

    def test_product_variadic_and_list(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Product("a", "b")) == [2.0, 8.0, 27.0]
        assert apply(lf, Product(["a", "b"])) == [2.0, 8.0, 27.0]

    def test_product_empty_is_one(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Product()) == [1, 1, 1]

    def test_output_alias(self, lf: pl.LazyFrame) -> None:
        out = collect(lf.with_columns(Add("a", "b", output="total")(lf)))
        assert out["total"].to_list() == [3.0, 6.0, 12.0]


class TestShortcutsAndConditional:
    def test_col_column_and_literal(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Col("a")) == [2.0, 4.0, 9.0]
        assert apply(lf, Col("not_a_column")) == ["not_a_column"] * 3

    def test_const(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Const("kg")) == ["kg"] * 3
        assert apply(lf, Const(1)) == [1, 1, 1]

    def test_replace(self, lf: pl.LazyFrame) -> None:
        from open_icu.callbacks._callbacks.comparison import GreaterThan

        callback = Replace(GreaterThan("a", 3), 0, "a")
        assert apply(lf, callback) == [2.0, 0.0, 0.0]


class TestSelectors:
    def test_first_not_null(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, FirstNotNull("maybe", "a", output=None)) == [2.0, 5.0, 9.0]
        assert apply(lf, FirstNotNull(["maybe", "a"], output=None)) == [2.0, 5.0, 9.0]

    def test_first_not_null_empty(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, FirstNotNull(output=None)) == [None, None, None]

    def test_max(self, lf: pl.LazyFrame) -> None:
        assert apply(lf, Max("a", "b", output=None)) == [2.0, 4.0, 9.0]
        assert apply(lf, Max(["a", "b"], output=None)) == [2.0, 4.0, 9.0]


class TestCast:
    def test_cast_string_to_int(self, lf: pl.LazyFrame) -> None:
        # strict=False (default): non-castable values become null
        assert apply(lf, Cast("text", "int64")) == [1, 2, None]

    def test_cast_strict_raises(self, lf: pl.LazyFrame) -> None:
        with pytest.raises(pl.exceptions.InvalidOperationError):
            lf.with_columns(Cast("text", "int64", strict=True)(lf).alias("out")).collect()

    def test_cast_unknown_dtype(self, lf: pl.LazyFrame) -> None:
        with pytest.raises(ValueError, match="Unsupported dtype"):
            Cast("a", "complex128")(lf)


class TestFilters:
    def test_drop_na(self, lf: pl.LazyFrame) -> None:
        out = collect(lf.filter(DropNa("maybe")(lf)))
        assert out["maybe"].to_list() == [5.0]

    def test_drop_if(self, lf: pl.LazyFrame) -> None:
        from open_icu.callbacks._callbacks.comparison import GreaterThan

        out = collect(lf.filter(DropIf(GreaterThan("a", 3))(lf)))
        assert out["a"].to_list() == [2.0]

    def test_first_distinct(self) -> None:
        lf = pl.LazyFrame({"id": [1, 1, 2], "t": [1, 1, 1], "v": [10, 20, 30]})
        out = collect(lf.filter(FirstDistinct("id", "t")(lf)))
        assert out["v"].to_list() == [10, 30]

    def test_first_distinct_list_form(self) -> None:
        lf = pl.LazyFrame({"id": [1, 1, 2], "t": [1, 1, 1], "v": [10, 20, 30]})
        out = collect(lf.filter(FirstDistinct(["id", "t"])(lf)))
        assert out["v"].to_list() == [10, 30]


class TestTime:
    def test_to_datetime_from_components(self) -> None:
        lf = pl.LazyFrame({"y": [2024], "m": [3], "d": [5], "t": ["14:30:00"]})
        result = apply(lf, ToDatetime("y", "m", "d", "t"))
        assert result == [datetime(2024, 3, 5, 14, 30)]

    @pytest.mark.parametrize(
        ("unit", "expected"),
        [
            ("minutes", datetime(2024, 3, 5, 14, 40)),
            ("hours", datetime(2024, 3, 6, 0, 30)),
            ("days", datetime(2024, 3, 15, 14, 30)),
            ("months", datetime(2025, 1, 5, 14, 30)),
            ("years", datetime(2034, 3, 5, 14, 30)),
        ],
    )
    def test_to_datetime_with_offset(self, unit: str, expected: datetime) -> None:
        lf = pl.LazyFrame({"y": [2024], "m": [3], "d": [5], "t": ["14:30:00"], "off": [10]})
        assert apply(lf, ToDatetime("y", "m", "d", "t", "off", offset_unit=unit)) == [expected]

    def test_to_datetime_invalid_unit(self) -> None:
        lf = pl.LazyFrame({"y": [2024], "m": [3], "d": [5], "t": ["14:30:00"], "off": [10]})
        with pytest.raises(ValueError, match="Unsupported offset_unit"):
            ToDatetime("y", "m", "d", "t", "off", offset_unit="fortnights")(lf)

    def test_add_offset(self) -> None:
        lf = pl.LazyFrame({"dt": [datetime(2024, 1, 1)], "off": [90]})
        assert apply(lf, AddOffset("dt", "off")) == [datetime(2024, 1, 1, 1, 30)]
        assert apply(lf, AddOffset("dt", "off", offset_unit="seconds")) == [datetime(2024, 1, 1, 0, 1, 30)]

    def test_add_offset_negative(self) -> None:
        lf = pl.LazyFrame({"dt": [datetime(2024, 1, 1)], "off": [-60]})
        assert apply(lf, AddOffset("dt", "off")) == [datetime(2023, 12, 31, 23, 0)]

    def test_add_offset_invalid_unit(self) -> None:
        lf = pl.LazyFrame({"dt": [datetime(2024, 1, 1)], "off": [1]})
        with pytest.raises(ValueError, match="Unsupported offset_unit"):
            AddOffset("dt", "off", offset_unit="decades")(lf)

    def test_set_time(self) -> None:
        lf = pl.LazyFrame({"dt": [datetime(2024, 5, 6, 1, 2, 3)]})
        assert apply(lf, SetTime("dt", 23, 59, 0)) == [datetime(2024, 5, 6, 23, 59, 0)]


class TestReshape:
    def test_split_explode(self) -> None:
        lf = pl.LazyFrame({"drug": ["a, b", "c"], "id": [1, 2]})
        out = SplitExplode("drug")(lf).collect()
        assert out["drug"].to_list() == ["a", "b", "c"]
        assert out["id"].to_list() == [1, 1, 2]

    def test_split_explode_no_strip(self) -> None:
        lf = pl.LazyFrame({"drug": ["a, b"]})
        out = SplitExplode("drug", strip=False)(lf).collect()
        assert out["drug"].to_list() == ["a", " b"]

    def test_split_explode_custom_separator(self) -> None:
        lf = pl.LazyFrame({"drug": ["a;b"]})
        out = SplitExplode("drug", separator=";")(lf).collect()
        assert out["drug"].to_list() == ["a", "b"]
