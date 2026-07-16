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
    """Keep the first row for each distinct combination of columns."""

    def __init__(self, *columns: AstValue) -> None:
        """
        Initialize the callback with the column names used to identify duplicates.

        Column names can be passed as separate arguments, for example:
        ``first_distinct(subject_id, time)``.
        """
        if len(columns) == 1 and isinstance(columns[0], list):
            self.columns: Sequence[AstValue] = columns[0]  # ty: ignore[invalid-assignment]
        else:
            self.columns = columns

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        """Select the first row for each distinct combination of column values."""
        column_names = [to_col_name(column) for column in self.columns]
        return pl.struct(column_names).is_first_distinct()


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
