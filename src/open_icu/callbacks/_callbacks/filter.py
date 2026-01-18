
import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_col_name
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class DropNa(CallbackProtocol):
    def __init__(self, column: AstValue) -> None:
        self.column = to_col_name(column)

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return pl.col(self.column).drop_nulls()
