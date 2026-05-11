
from typing import Any, Optional, Sequence

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_col_name
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
        self.fields = fields

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        """
        """
        cols = [to_col_name(f) for f in self.fields]
        return pl.struct(cols).is_first_distinct()