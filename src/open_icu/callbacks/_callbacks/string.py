from typing import Optional

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_expr
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class ZeroPadInt(CallbackProtocol):
    """Zero-pad the integer portion of a numeric string to a fixed width.

    Casts the input to string and left-pads the portion before any
    decimal point to `width` characters, leaving a fractional suffix
    (e.g. ".531") intact. Handles fixed-width encoded fields (HHMMSS,
    MMDD, ...) that arrive as floats and have lost leading zeros,
    without discarding sub-second precision.

    Attributes:
        value: Column or literal to pad.
        width: Target width of the integer portion, in characters.
        round_ndigits: If set, round the value to this many decimal
            places before stringifying, to strip float64 representation
            noise (e.g. 213014.53100000002 -> 213014.531).
        output: Output column name. If None, the expression is
            returned unaliased.
    """

    def __init__(
        self,
        value: AstValue,
        width: int,
        round_ndigits: Optional[int] = None,
        output: Optional[str] = None,
    ) -> None:
        self.value = value
        self.width = width
        self.round_ndigits = round_ndigits
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        e = to_expr(lf, self.value)
        if self.round_ndigits is not None:
            e = e.round(self.round_ndigits)
        s = e.cast(pl.Utf8)

        split = s.str.split_exact(".", 1)
        int_part = split.struct.field("field_0").str.zfill(self.width)
        frac_part = split.struct.field("field_1")

        expr = (
            pl.when(frac_part.is_not_null())
            .then(int_part + pl.lit(".") + frac_part)
            .otherwise(int_part)
        )
        return expr if self.output is None else expr.alias(self.output)


@register_callback_cls
class ConcatStr(CallbackProtocol):
    """Concatenate one or more columns/literals into a single string.

    Attributes:
        parts: Values to concatenate, in order, each cast to string.
        separator: String inserted between parts. Defaults to "" (no
            separator, e.g. for date/time concatenation).
        output: Output column name. If None, the expression is
            returned unaliased.
        ignore_nulls: If True, rows with a null part produce null for
            the whole result (pl.concat_str default). If False,
            missing parts are treated as empty strings and joining
            proceeds with the remaining parts.
    """

    def __init__(
        self,
        *parts: AstValue,
        separator: str = "",
        output: Optional[str] = None,
        ignore_nulls: bool = False,
    ) -> None:
        self.parts = parts
        self.separator = separator
        self.output = output
        self.ignore_nulls = ignore_nulls

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = pl.concat_str(
            [to_expr(lf, part).cast(pl.Utf8) for part in self.parts],
            separator=self.separator,
            ignore_nulls=self.ignore_nulls,
        )
        return expr if self.output is None else expr.alias(self.output)


@register_callback_cls
class SliceStr(CallbackProtocol):
    """Slice a substring out of a string column or literal.

    Attributes:
        value: Column or literal to slice.
        offset: Start index of the slice. Negative values count from
            the end of the string (polars semantics).
        length: Number of characters to take from offset. If None,
            slices to the end of the string.
        output: Output column name. If None, the expression is
            returned unaliased.
    """

    def __init__(
        self,
        value: AstValue,
        offset: int,
        length: Optional[int] = None,
        output: Optional[str] = None,
    ) -> None:
        self.value = value
        self.offset = offset
        self.length = length
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = to_expr(lf, self.value).cast(pl.Utf8).str.slice(self.offset, self.length)
        return expr if self.output is None else expr.alias(self.output)
