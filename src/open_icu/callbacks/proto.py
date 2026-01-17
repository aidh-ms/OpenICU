from typing import Any, Protocol, Union, runtime_checkable

import polars as pl
from polars import Expr, LazyFrame

CallbackResult = Union[LazyFrame, Expr]

@runtime_checkable
class CallbackProtocol(Protocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        ...

AstAtom = Union[int, float, bool, str, None]
AstValue = Union[AstAtom, Expr, CallbackProtocol]

def to_expr(lf: LazyFrame, value: AstValue) -> Expr:
    if isinstance(value, Expr):
        return value

    if isinstance(value, str):
        return pl.col(value)

    if isinstance(value, (int, float, bool)) or value is None:
        return pl.lit(value)

    if isinstance(value, CallbackProtocol):
        out = value(lf)
        if not isinstance(out, Expr):
            raise TypeError(
                f"Nested callback '{type(value).__name__}' must return polars.Expr, "
                f"but returned {type(out).__name__}."
            )
        return out

    raise TypeError(f"Cannot convert {type(value).__name__} to polars.Expr")
