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

    result: str

    def as_expression(self) -> Expr:
        raise NotImplementedError("Subclasses must implement this method.")

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(self.as_expression().alias(self.result))

class FrameCallback(CallbackProtocol):
    """Callback that apply directly to LazyFrame. 
    Cannot be used in ast."""

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        raise NotImplementedError("Subclasses must implement this method.")

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return self.as_field(lf)
    
class HybridCallback(ExpressionCallback, FrameCallback):
    """Callback that can be used either directly on LazyFrame or in ast."""

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return self.as_field(lf)