import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import CallbackProtocol
from open_icu.callbacks.registry import register_callback_class


@register_callback_class
class ToDatetime(CallbackProtocol):
    def __init__(self, year: str, month: str, day: str, time: str, offset: str, output: str) -> None:
        self.year = year
        self.month = month
        self.day = day
        self.time = time
        self.offset = offset
        self.output = output

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        datetime_expr = (
            pl.col(self.year).cast(pl.Utf8).str.zfill(4) + pl.lit("-") +
            pl.col(self.month).cast(pl.Utf8).str.zfill(2) + pl.lit("-") +
            pl.col(self.day).cast(pl.Utf8).str.zfill(2) + pl.lit(" ") +
            pl.col(self.time).cast(pl.Utf8)
        ).str.to_datetime()
        offset_expr = pl.duration(minutes=pl.col(self.offset).abs())

        return lf.with_columns(
            (datetime_expr + offset_expr).alias(self.output)
        )


@register_callback_class
class AddOffset(CallbackProtocol):
    def __init__(self, datetime: str, offset: str, output: str) -> None:
        self.datetime = datetime
        self.offset = offset
        self.output = output

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        offset_expr = pl.duration(minutes=pl.col(self.offset).abs())

        return lf.with_columns(
            (pl.col(self.datetime) + offset_expr).alias(self.output)
        )
