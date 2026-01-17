from typing import Optional

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_expr
from open_icu.callbacks.registry import register_callback_class


@register_callback_class
class ToDatetime(CallbackProtocol):
    def __init__(
        self,
        year: AstValue,
        month: AstValue,
        day: AstValue,
        time: AstValue,
        offset: AstValue,
        output: Optional[str] = None,
    ) -> None:
        self.year = year
        self.month = month
        self.day = day
        self.time = time
        self.offset = offset
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        datetime_expr = (
            to_expr(lf, self.year).cast(pl.Utf8).str.zfill(4)
            + pl.lit("-")
            + to_expr(lf, self.month).cast(pl.Utf8).str.zfill(2)
            + pl.lit("-")
            + to_expr(lf, self.day).cast(pl.Utf8).str.zfill(2)
            + pl.lit(" ")
            + to_expr(lf, self.time).cast(pl.Utf8)
        ).str.to_datetime()
        offset_expr = pl.duration(minutes=to_expr(lf, self.offset).abs())
        expr = datetime_expr + offset_expr
        return expr if self.output is None else lf.with_columns(expr.alias(self.output))


@register_callback_class
class AddOffset(CallbackProtocol):
    def __init__(self, datetime: AstValue, offset: AstValue, output: Optional[str] = None) -> None:
        self.datetime = datetime
        self.offset = offset
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        offset_expr = pl.duration(minutes=to_expr(lf, self.offset).abs())
        expr = to_expr(lf, self.datetime) + offset_expr

        return expr if self.output is None else lf.with_columns(expr.alias(self.output))
