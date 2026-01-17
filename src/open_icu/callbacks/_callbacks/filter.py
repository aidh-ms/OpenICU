from typing import Sequence

from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_col_name
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class DropNa(CallbackProtocol):
    def __init__(self, *columns: AstValue) -> None:
        if len(columns) == 1 and isinstance(columns[0], list):
            self.columns: Sequence[AstValue] = columns[0]
        else:
            self.columns = columns

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        subset: list[str] = [to_col_name(c) for c in self.columns]

        if not subset:
            return lf.drop_nulls()

        return lf.drop_nulls(subset=subset)
