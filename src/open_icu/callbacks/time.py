import polars as pl
from polars import Expr

from open_icu.callbacks.proto import ExpressionCallback
from open_icu.callbacks.registry import register_callback_class


@register_callback_class
class ToDatetime(ExpressionCallback):
    def __init__(self, year: str, month: str, day: str, time: str, offset: str, output: str | None = None) -> None:
        self.year = year
        self.month = month
        self.day = day
        self.time = time
        self.offset = offset
        if output is not None:
            self.output = output
    
    def as_expression(self) -> Expr:
        datetime_expr = (
            self.year.cast(pl.Utf8).str.zfill(4) + pl.lit("-") +
            self.month.cast(pl.Utf8).str.zfill(2) + pl.lit("-") +
            self.day.cast(pl.Utf8).str.zfill(2) + pl.lit(" ") +
            self.time.cast(pl.Utf8)
        ).str.to_datetime()
        offset_expr = pl.duration(minutes=self.offset.abs())

        return (datetime_expr + offset_expr)


@register_callback_class
class AddOffset(ExpressionCallback):
    def __init__(self, datetime: str, offset: str, output: str | None = None) -> None:
        self.datetime = datetime
        self.offset = offset
        if output is not None:
            self.output = output
    
    def as_expression(self) -> Expr:
        offset_expr = pl.duration(minutes=self.offset.abs())

        return self.datetime + offset_expr