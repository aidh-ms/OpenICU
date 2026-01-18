from typing import Optional

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_expr
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class Col(CallbackProtocol):
    def __init__(
        self,
        col: AstValue,
        output: Optional[str] = None,
    ) -> None:
        self.col = col
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = to_expr(lf, self.col)
        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class Const(CallbackProtocol):
    def __init__(
        self,
        value: AstValue,
        output: Optional[str] = None,
    ) -> None:
        self.value = value
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = pl.lit(self.value)
        if self.output is None:
            return expr
        return expr.alias(self.output)
