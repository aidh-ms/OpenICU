from typing import Any, Protocol, Union, runtime_checkable

import polars as pl
from polars import LazyFrame

AstAtom = Union[int, float, bool, str, None]
AstValue = Union[AstAtom, pl.Expr, "CallbackProtocol", list["AstValue"]]
CallbackResult = pl.Expr


@runtime_checkable
class CallbackProtocol(Protocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

    def __call__(self, lf: LazyFrame) -> CallbackResult: ...


def to_expr(lf: LazyFrame, value: AstValue) -> pl.Expr:
    if isinstance(value, pl.Expr):
        return value

    if isinstance(value, str):
        return pl.col(value)

    if isinstance(value, (int, float, bool)) or value is None:
        return pl.lit(value)

    if isinstance(value, CallbackProtocol):
        out = value(lf)
        if not isinstance(out, pl.Expr):
            raise TypeError(
                f"Nested callback '{type(value).__name__}' must return polars.pl.Expr, but returned {type(out).__name__}."
            )
        return out

    raise TypeError(f"Cannot convert {type(value).__name__} to polars.pl.Expr")


def to_col_name(v: AstValue) -> str:
    if isinstance(v, str):
        return v
    raise TypeError(f"Expected column name (str), got {type(v).__name__}")
