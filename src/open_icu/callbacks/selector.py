import polars as pl
from polars import LazyFrame, Expr

from open_icu.callbacks.proto import CallbackProtocol, ExpressionCallback
from open_icu.callbacks.registry import register_callback_class


@register_callback_class
class FirstNotNull(ExpressionCallback):
    """Create a column by selecting the first non-null value across columns.

    This callback evaluates a list of source columns row-wise and assigns the
    first non-null value (in order) to a new result column. If all specified
    fields are null for a row, the resulting value is null. It is commonly used
    for schema harmonization.
    """

    def __init__(self, fields:list[str], result: str) -> None:
        """Initialize the callback.

        Args:
            fields: Ordered list of source column names. Earlier columns
                take precedence over later ones.
            result: Name of the output column to be created.
        """
        self.fields = fields
        self.result = result

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        """Apply the transformation to a Polars LazyFrame.

        Args:
            lf: Input LazyFrame.

        Returns:
            A new LazyFrame with an additional column containing the first
            non-null value per row across the specified fields.
        """
        return lf.with_columns(
            pl.coalesce([pl.col(col) for col in self.fields]).alias(self.result)
        )
    
    def as_expression(self) -> Expr:
        return pl.coalesce([pl.col(col) for col in self.fields]).alias(self.result)