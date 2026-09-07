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
        offset_unit: str = "minutes",
        output: Optional[str] = None,
    ) -> None:
        self.datetime = datetime
        self.offset = offset
        self.offset_unit = offset_unit
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        offset_expr = to_expr(lf, self.offset)

        if self.offset_unit not in [
            "weeks",
            "days",
            "hours",
            "minutes",
            "seconds",
            "milliseconds",
            "microseconds",
            "nanoseconds",
        ]:
            raise ValueError(f"Unsupported offset_unit: {self.offset_unit}")

        expr = to_expr(lf, self.datetime) + pl.duration(**{self.offset_unit: offset_expr})  # ty: ignore[invalid-argument-type]

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


@register_callback_cls
class ParseDateTime(CallbackProtocol):
    """Concatenate one or more columns/literals into a datetime string and parse it.

    Attributes:
        parts: Values to concatenate, in order, each cast to string as-is.
            Use ZeroPadInt upstream for any part needing zero-padding.
        format: strptime format string matching the concatenated result.
            Use "%.f" to consume an optional fractional-seconds suffix.
        output: Output column name. If None, the expression is
            returned unaliased.
        strict: Passed through to str.strptime; False turns unparseable
            values into null instead of raising.
    """

    def __init__(
        self,
        *parts: AstValue,
        format: str,
        output: Optional[str] = None,
        strict: bool = False,
    ) -> None:
        self.parts = parts
        self.format = format
        self.output = output
        self.strict = strict

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = pl.concat_str(
            [to_expr(lf, part).cast(pl.Utf8) for part in self.parts],
            separator="",
        ).str.strptime(pl.Datetime, format=self.format, strict=self.strict)

        if self.output is None:
            return expr
        return expr.alias(self.output)
