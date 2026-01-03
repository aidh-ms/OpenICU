from polars import LazyFrame

from open_icu.callbacks.proto import CallbackProtocol
from open_icu.callbacks.registry import register_callback_class


@register_callback_class
class DropNa(CallbackProtocol):
    def __init__(self, columns: list[str]) -> None:
        self.columns = columns

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.drop_nulls(subset=self.columns)
