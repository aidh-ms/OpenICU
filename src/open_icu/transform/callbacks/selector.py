import polars as pl
from polars import LazyFrame

from open_icu.transform.callbacks.proto import CallbackProtocol
from open_icu.transform.callbacks.registry import register_callback_class


@register_callback_class
class FirstNotNull(CallbackProtocol):
    def __init__(self, fields:list[str], result: str) -> None:
        self.fields = fields
        self.result = result

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        
        return lf.with_columns(
            pl.coalesce([pl.col(col) for col in self.fields]).alias(self.result)
        )