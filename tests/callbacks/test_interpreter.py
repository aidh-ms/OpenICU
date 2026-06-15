"""Tests for the expression DSL interpreter (open_icu.callbacks.interpreter)."""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from open_icu.callbacks.interpreter import ExprInterpreter, parse_expr
from open_icu.callbacks.proto import CallbackProtocol


@pytest.fixture
def lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "a": [1.0, 2.0, 3.0],
            "b": [10.0, 20.0, 30.0],
            "flag": [True, False, True],
            "label": ["x", "y", "z"],
        }
    )


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    df = lf.collect()
    assert isinstance(df, pl.DataFrame)
    return df


def evaluate(lf: pl.LazyFrame, expr: str, name: str = "result") -> pl.Series:
    return collect(lf.with_columns(parse_expr(lf, expr).alias(name)))[name]


class TestLiteralsAndNames:
    def test_constant(self) -> None:
        assert ExprInterpreter().eval("42") == 42
        assert ExprInterpreter().eval("'text'") == "text"
        assert ExprInterpreter().eval("None") is None

    def test_list_and_tuple(self) -> None:
        assert ExprInterpreter().eval("[1, 2, 3]") == [1, 2, 3]
        assert ExprInterpreter().eval("(1, 2)") == (1, 2)

    def test_bare_name_is_column_reference(self, lf: pl.LazyFrame) -> None:
        # A bare name inside a callback resolves to the column of that name.
        assert evaluate(lf, "col(a)").to_list() == [1.0, 2.0, 3.0]

    def test_unknown_name_falls_back_to_literal(self, lf: pl.LazyFrame) -> None:
        # Names that are not columns of the frame are treated as string literals.
        assert evaluate(lf, "col(missing)").to_list() == ["missing"] * 3


class TestOperators:
    @pytest.mark.parametrize(
        ("expr", "expected"),
        [
            ("col(a) + col(b)", [11.0, 22.0, 33.0]),
            ("col(b) - col(a)", [9.0, 18.0, 27.0]),
            ("col(a) * 2", [2.0, 4.0, 6.0]),
            ("col(b) / col(a)", [10.0, 10.0, 10.0]),
            ("-col(a)", [-1.0, -2.0, -3.0]),
            ("+col(a)", [1.0, 2.0, 3.0]),
        ],
    )
    def test_arithmetic(self, lf: pl.LazyFrame, expr: str, expected: list[float]) -> None:
        assert evaluate(lf, expr).to_list() == expected

    @pytest.mark.parametrize(
        ("expr", "expected"),
        [
            ("col(a) > 1", [False, True, True]),
            ("col(a) >= 2", [False, True, True]),
            ("col(a) < 3", [True, True, False]),
            ("col(a) <= 2", [True, True, False]),
            ("col(a) == 2", [False, True, False]),
            ("col(a) != 2", [True, False, True]),
        ],
    )
    def test_comparisons(self, lf: pl.LazyFrame, expr: str, expected: list[bool]) -> None:
        assert evaluate(lf, expr).to_list() == expected

    @pytest.mark.parametrize(
        ("expr", "expected"),
        [
            ("col(flag) & (col(a) > 1)", [False, False, True]),
            ("col(flag) | (col(a) > 1)", [True, True, True]),
            ("col(flag) and col(a) > 1", [False, False, True]),
            ("col(flag) or col(a) > 1", [True, True, True]),
            ("not col(flag)", [False, True, False]),
            ("~col(flag)", [False, True, False]),
        ],
    )
    def test_boolean_logic(self, lf: pl.LazyFrame, expr: str, expected: list[bool]) -> None:
        assert evaluate(lf, expr).to_list() == expected

    def test_multi_value_bool_op(self, lf: pl.LazyFrame) -> None:
        result = evaluate(lf, "col(flag) and col(a) > 1 and col(b) >= 30")
        assert result.to_list() == [False, False, True]


class TestCalls:
    def test_nested_callbacks(self, lf: pl.LazyFrame) -> None:
        result = evaluate(lf, "replace(col(a) > 1, multiply(col(a), 10), col(a))")
        assert result.to_list() == [1.0, 20.0, 30.0]

    def test_output_keyword_aliases_column(self, lf: pl.LazyFrame) -> None:
        out = collect(lf.with_columns(parse_expr(lf, "add(col(a), col(b), output=total)")))
        assert out["total"].to_list() == [11.0, 22.0, 33.0]

    def test_unknown_callback_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown callback"):
            ExprInterpreter().eval("definitely_not_registered(a)")

    def test_bad_arguments_raise_type_error(self) -> None:
        with pytest.raises(TypeError, match="Bad arguments"):
            ExprInterpreter().eval("add(1, 2, 3, 4, 5)")

    def test_method_calls_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="Only simple calls allowed"):
            ExprInterpreter().eval("col(a).alias('b')")


class TestUnsupportedSyntax:
    def test_chained_comparison_rejected(self) -> None:
        with pytest.raises(NotImplementedError):
            ExprInterpreter().eval("1 < col(a) < 3")

    @pytest.mark.parametrize("expr", ["lambda x: x", "{'a': 1}", "[x for x in y]", "col(a) if flag else col(b)"])
    def test_unsupported_syntax_rejected(self, expr: str) -> None:
        with pytest.raises(ValueError, match="Unsupported syntax"):
            ExprInterpreter().eval(expr)

    def test_unsupported_binop_rejected(self) -> None:
        with pytest.raises(NotImplementedError):
            ExprInterpreter().eval("col(a) @ col(b)")


class TestParseExpr:
    def test_returns_polars_expr(self, lf: pl.LazyFrame) -> None:
        assert isinstance(parse_expr(lf, "col(a)"), pl.Expr)

    def test_top_level_must_be_callback(self, lf: pl.LazyFrame) -> None:
        # Bare literals/names are not valid top-level expressions.
        with pytest.raises(AssertionError):
            parse_expr(lf, "42")

    def test_interpreter_output_is_callback(self) -> None:
        callback = ExprInterpreter().eval("col(a)")
        assert isinstance(callback, CallbackProtocol)

    def test_filter_usage(self, lf: pl.LazyFrame) -> None:
        filtered = collect(lf.filter(parse_expr(lf, "col(a) >= 2")))
        assert_frame_equal(filtered, collect(lf).filter(pl.col("a") >= 2))
