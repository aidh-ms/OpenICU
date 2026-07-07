
from typing import Sequence

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_col_name, to_expr
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class DropNa(CallbackProtocol):
    def __init__(self, column: AstValue) -> None:
        self.column = to_col_name(column)

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return pl.col(self.column).is_not_null()



@register_callback_cls
class FirstDistinct(CallbackProtocol):
    """
    """

    def __init__(self, *fields: AstValue) -> None:
        """
        """
        if len(fields) == 1 and isinstance(fields[0], list):
            self.fields: Sequence[AstValue] = fields[0]  # ty: ignore[invalid-assignment]
        else:
            self.fields = fields

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        """
        """
        cols = [to_col_name(f) for f in self.fields]
        return pl.struct(cols).is_first_distinct()

@register_callback_cls
class DropIf(CallbackProtocol):
    """
    Drops rows where the given boolean condition is True.

    Example:

    Meaning:
        keep rows where is_invalid is False
    """

    def __init__(self, condition: AstValue) -> None:
        self.condition = condition

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        condition_expr = to_expr(lf, self.condition)
        return condition_expr.not_()
