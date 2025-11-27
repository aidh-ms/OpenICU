from typing import Any, Protocol

from polars import LazyFrame, Expr


class CallbackProtocol(Protocol):
    def __init__(self, **kwargs: Any) -> None:
        ...

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        ...

class ExpressionCallback(CallbackProtocol):
    """Callbacks that produce a expression. 
    Can be used in abstract syntax tree (ast)."""

    def as_expression(self) -> Expr:
        ...

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(self.as_expression())

    def __init__(self, **kwargs: Any) -> None:
        ...


class FrameCallback(CallbackProtocol):
    """Callback that apply directly to LazyFrame. 
    Cannot be used in ast."""

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        ...

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return self.as_field(lf)
    
class HybridCallback(CallbackProtocol):
    """Callback that can be used either directly on LazyFrame or in ast."""

    def as_expression(self) -> Expr:
        ...

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(self.as_expression())

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return self.as_field(lf)