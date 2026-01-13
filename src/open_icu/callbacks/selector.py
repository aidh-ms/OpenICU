import polars as pl
from polars import Expr

from open_icu.callbacks.proto import ExpressionCallback
from open_icu.callbacks.registry import register_callback_class


@register_callback_class
class FirstNotNull(ExpressionCallback):
    """Create a column by selecting the first non-null value across columns.

    This callback evaluates a list of source columns row-wise and assigns the
    first non-null value (in order) to a new result column. If all specified
    fields are null for a row, the resulting value is null. It is commonly used
    for schema harmonization.
    """

    def __init__(self, fields:list[str], output: str) -> None:
        """Initialize the callback.

        Args:
            fields: Ordered list of source column names. Earlier columns
                take precedence over later ones.
            result: Name of the output column to be created.
        """
        self.fields = fields
        if output is not None:
            self.output = output
    
    def as_expression(self) -> Expr:
        return pl.coalesce([pl.col(col) for col in self.fields]).alias(self.output)