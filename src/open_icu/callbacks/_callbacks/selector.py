from typing import Optional, Sequence

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_col_name
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class FirstNotNull(CallbackProtocol):
    """Create a column by selecting the first non-null value across columns.

    This callback evaluates a list of source columns row-wise and assigns the
    first non-null value (in order) to a new result column. If all specified
    fields are null for a row, the resulting value is null. It is commonly used
    for schema harmonization.
    """

    def __init__(self, *fields: AstValue, output: Optional[str]) -> None:
        """Initialize the callback.

        Args:
            fields: Ordered list of source column names. Earlier columns
                take precedence over later ones.
            output: Name of the output column to be created.
        """
        if len(fields) == 1 and isinstance(fields[0], list):
            self.fields: Sequence[AstValue] = fields[0]
        else:
            self.fields = fields

        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        """Apply the transformation to a Polars LazyFrame.

        Args:
            lf: Input LazyFrame.

        Returns:
            A new LazyFrame with an additional column containing the first
            non-null value per row across the specified fields.
        """
        cols = [to_col_name(f) for f in self.fields]

        # Optional: empty list handling
        if not cols:
            # With no inputs, output is always null.
            expr = pl.lit(None)
            # return expr if self.output is None else lf.with_columns(expr.alias(self.output))
            if self.output is None:
                return expr
            return expr.alias(self.output)

        expr = pl.coalesce([pl.col(c) for c in cols])

        # return expr if self.output is None else lf.with_columns(expr.alias(self.output))
        if self.output is None:
            return expr
        return expr.alias(self.output)
