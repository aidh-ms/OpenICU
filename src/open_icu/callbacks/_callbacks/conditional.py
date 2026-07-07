from typing import Optional

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_expr
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class Replace(CallbackProtocol):
    def __init__(
        self,
        condition: AstValue,
        then_value: AstValue,
        else_value: AstValue,
        output: Optional[str] = None,
    ) -> None:
        self.condition = condition
        self.then_value = then_value
        self.else_value = else_value
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = (
            pl.when(to_expr(lf, self.condition))
            .then(to_expr(lf, self.then_value))
            .otherwise(to_expr(lf, self.else_value))
        )

        if self.output is None:
            return expr

        return expr.alias(self.output)
