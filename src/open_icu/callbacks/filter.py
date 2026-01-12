from polars import LazyFrame, Expr


from open_icu.callbacks.proto import CallbackProtocol, ExpressionCallback, FrameCallback
from open_icu.callbacks.registry import register_callback_class


@register_callback_class
class DropNa(FrameCallback):
    def __init__(self, columns: list[str]) -> None:
        self.columns = columns

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.drop_nulls(subset=self.columns)
    

