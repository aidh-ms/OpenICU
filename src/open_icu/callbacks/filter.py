from polars import LazyFrame


from open_icu.callbacks.proto import FrameCallback
from open_icu.callbacks.registry import register_callback_class


@register_callback_class
class DropNa(FrameCallback):
    def __init__(self, columns: list[str]) -> None:
        self.columns = columns

    def as_field(self, lf) -> LazyFrame:
        return lf.drop_nulls(subset=self.columns)