from typing import Optional

import polars as pl
from click import Option
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_expr
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
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
        self.year: AstValue = year
        self.month: AstValue = month
        self.day: AstValue = day
        self.time: AstValue = time
        self.offset: AstValue = offset
        self.output: Option[str] = output

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
        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class AddOffset(CallbackProtocol):
    def __init__(self, datetime: AstValue, offset: AstValue, output: Optional[str] = None) -> None:
        self.datetime = datetime
        self.offset = offset
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        offset_expr = pl.duration(minutes=to_expr(lf, self.offset).abs())
        expr = to_expr(lf, self.datetime) + offset_expr

        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class SetTime(CallbackProtocol):
    def __init__(
        self,
        datetime: AstValue,
        hours: AstValue,
        minutes: AstValue,
        seconds: AstValue,
        output: Optional[str] = None,
    ) -> None:
        self.datetime: AstValue = datetime
        self.hours: AstValue = hours
        self.minutes: AstValue = minutes
        self.seconds: AstValue = seconds
        self.output: Optional[str] = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        dt_expr = to_expr(lf, self.datetime)

        expr = pl.datetime(
            year=dt_expr.dt.year(),
            month=dt_expr.dt.month(),
            day=dt_expr.dt.day(),
            hour=to_expr(lf, self.hours),
            minute=to_expr(lf, self.minutes),
            second=to_expr(lf, self.seconds),
        )

        if self.output is None:
            return expr
        return expr.alias(self.output)
