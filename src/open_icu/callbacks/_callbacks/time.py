from typing import Optional

import polars as pl
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
        offset: Optional[AstValue] = None,
        offset_unit: str = "minutes",
        output: Optional[str] = None,
    ) -> None:
        self.year = year
        self.month = month
        self.day = day
        self.time = time
        self.offset = offset
        self.offset_unit = offset_unit
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

        expr = datetime_expr

        if self.offset is not None:
            offset_expr = to_expr(lf, self.offset)

            if self.offset_unit == "minutes":
                expr = expr + pl.duration(minutes=offset_expr)
            elif self.offset_unit == "hours":
                expr = expr + pl.duration(hours=offset_expr)
            elif self.offset_unit == "days":
                expr = expr + pl.duration(days=offset_expr)
            elif self.offset_unit == "years":
                expr = expr.dt.offset_by(offset_expr.cast(pl.String) + pl.lit("y"))
            elif self.offset_unit == "months":
                expr = expr.dt.offset_by(offset_expr.cast(pl.String) + pl.lit("mo"))
            else:
                raise ValueError(f"Unsupported offset_unit: {self.offset_unit}")

        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class AddOffset(CallbackProtocol):
    def __init__(
        self,
        datetime: AstValue,
        offset: AstValue,
        output: Optional[str] = None,
        offset_unit: str = "minutes",
    ) -> None:
        self.datetime = datetime
        self.offset = offset
        self.output = output
        self.offset_unit = offset_unit


    def __call__(self, lf: LazyFrame) -> CallbackResult:
        offset_expr = to_expr(lf, self.offset)
        expr = to_expr(lf, self.datetime)

        if self.offset_unit == "minutes":
            expr = expr + pl.duration(minutes=offset_expr)
        elif self.offset_unit == "hours":
            expr = expr + pl.duration(hours=offset_expr)
        elif self.offset_unit == "days":
            expr = expr + pl.duration(days=offset_expr)
        else:
            raise ValueError(f"Unsupported offset_unit: {self.offset_unit}")

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
