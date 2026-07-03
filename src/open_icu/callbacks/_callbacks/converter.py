from typing import Optional

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_expr
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class ConvertUnit(CallbackProtocol):
    def __init__(
        self,
        value: AstValue,
        unit: AstValue,
        from_unit: AstValue,
        factor: AstValue,
        output: Optional[str] = None,
    ) -> None:
        self.value = value
        self.unit = unit
        self.from_unit = from_unit
        self.factor = factor
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        value_expr = to_expr(lf, self.value)
        unit_expr = to_expr(lf, self.unit)
        from_unit = to_expr(lf, self.from_unit)
        factor = to_expr(lf, self.factor)

        expr = (
            pl.when(unit_expr == from_unit)
            .then(value_expr * factor)
            .otherwise(value_expr)
        )

        if self.output is None:
            return expr
        return expr.alias(self.output)
