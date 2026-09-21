from typing import Optional

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_expr
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class DenseRankEncode(CallbackProtocol):
    """Map string (or any) ids to dense integer codes via rank.

    Uses `.rank(method="dense")` instead of casting, so it works for
    non-numeric ids and never leaves the query graph — no separate
    unique/join step, no collect. Ranks are deterministic for a given
    set of distinct values but only within this LazyFrame's evaluation
    (not stable across separately-processed frames/chunks).

    Attributes:
        id: Column/expression holding the id to encode.
        output: Output column name. If None, the expression is
            returned unaliased.
        descending: Passed through to rank's descending flag.
    """

    def __init__(
        self,
        id: AstValue,
        output: Optional[str] = None,
        descending: bool = False,
    ) -> None:
        self.id = id
        self.output = output
        self.descending = descending

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = to_expr(lf, self.id).rank(method="dense", descending=self.descending).cast(pl.Int64)

        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class DatetimeDiffDays(CallbackProtocol):
    """Compute the difference between two datetime columns in fractional days.

    Attributes:
        start: Column/expression for the earlier (or reference) datetime.
        end: Column/expression for the later datetime; the result is
            (end - start) in days.
        output: Output column name. If None, the expression is
            returned unaliased.
    """

    def __init__(
        self,
        start: AstValue,
        end: AstValue,
        output: Optional[str] = None,
    ) -> None:
        self.start = start
        self.end = end
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        duration_expr = to_expr(lf, self.end) - to_expr(lf, self.start)
        expr = duration_expr.dt.total_seconds() / 86400.0

        if self.output is None:
            return expr
        return expr.alias(self.output)
