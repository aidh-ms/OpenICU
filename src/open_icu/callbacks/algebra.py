import operator
from numbers import Real

import polars as pl
from polars import Expr

from open_icu.callbacks.proto import ExpressionCallback
from open_icu.callbacks.registry import register_callback_class


@register_callback_class
class Add(ExpressionCallback):
    """Add two columns or expressions.

    This hybrid callback supports both expression-level usage (e.g. inside
    an AST-expression) and frame-level usage, where the result is materialized
    as a new column.
    """

    def __init__(self, augend: str, addend: str, output: str | None = None) -> None:
        """Initialize the callback.

        Args:
            augend: Name of the first operand column.
            addend: Name of the second operand column.
            sum: Optional name of the output column.
        """
        self.augend = augend
        self.addend = addend
        if output is not None:
            self.output = output

    def as_expression(self) -> Expr:
        """Return the addition as a Polars expression.

        Returns:
            A Polars expression representing `augend + addend`.
        """
        return self.augend + self.addend  # type: ignore


@register_callback_class
class Sum(ExpressionCallback):
    """Compute the row-wise sum across multiple columns."""

    def __init__(self, summands: list[str], output: str | None = None) -> None:
        """Initialize the callback.

        Args:
            summands: List of column names to be summed row-wise.
            sum: Optional name of the output column.
        """
        self.summands = summands
        if output is not None:
            self.output = output

    def as_expression(self) -> Expr:
        """Return the horizontal sum as a Polars expression.

        Returns:
            A Polars expression computing the row-wise sum.
        """
        return pl.sum_horizontal(self.summands)


@register_callback_class
class Subtract(ExpressionCallback):
    """Subtract one column or expression from another."""

    def __init__(self, minuend: str, subtrahend: str, output: str | None = None) -> None:
        """Initialize the callback.

        Args:
            minuend: Name of the column to subtract from.
            subtrahend: Name of the column to subtract.
            difference: Optional name of the output column.
        """
        self.minuend = minuend
        self.subtrahend = subtrahend
        if output is not None:
            self.output = output

    def as_expression(self) -> Expr:
        """Return the subtraction as a Polars expression.

        Returns:
            A Polars expression representing `minuend - subtrahend`.
        """
        return self.minuend - self.subtrahend  # type: ignore


@register_callback_class
class Multiply(ExpressionCallback):
    """Multiply two columns or expressions."""

    def __init__(self, multiplicand: str, multiplier: str, output: str | None = None) -> None:
        """Initialize the callback.

        Args:
            multiplicand: Name of the first operand column.
            multiplier: Name of the second operand column.
            product: Optional name of the output column.
        """
        self.multiplicand = multiplicand
        self.multiplier = multiplier
        if output is not None:
            self.output = output

    def as_expression(self) -> Expr:
        """Return the multiplication as a Polars expression.

        Returns:
            A Polars expression representing `multiplicand * multiplier`.
        """
        return self.multiplicand * self.multiplier  # type: ignore


@register_callback_class
class Product(ExpressionCallback):
    """Compute the row-wise product across multiple columns.

    Note:
        This callback is intended for expression-level usage. Frame-level
        materialization is currently not implemented.
    """

    def __init__(self, factors: list[str], output: str | None = None) -> None:
        """Initialize the callback.

        Args:
            factors: List of column names whose values are multiplied row-wise.
            product: Optional name of the output column.
        """
        self.factors = factors
        if output is not None:
            self.output = output

    def as_expression(self) -> Expr:
        """Return the horizontal product as a Polars expression.

        Returns:
            A Polars expression computing the row-wise product.
        """
        return pl.fold(
            acc=pl.lit(1),
            function=operator.mul,
            exprs=self.factors,
        )        


@register_callback_class
class Divide(ExpressionCallback):
    """Divide one column or expression by another."""

    def __init__(self, dividend: str, divisor: str, output: str | None = None) -> None:
        """Initialize the callback.

        Args:
            dividend: Name of the numerator column.
            divisor: Name of the denominator column.
            quotient: Optional name of the output column.
        """
        self.dividend = dividend
        self.divisor = divisor
        if output is not None:
            self.output = output

    def as_expression(self) -> Expr:
        """Return the division as a Polars expression.

        Returns:
            A Polars expression representing `dividend / divisor`.
        """
        return self.dividend / self.divisor  # type: ignore


@register_callback_class
class Pow(ExpressionCallback):
    """Raise a column or expression to a power."""

    def __init__(self, base: str, exponent: str, output: str | None = None) -> None:
        """Initialize the callback.

        Args:
            base: Name of the base column.
            exponent: Name of the exponent column.
            power: Optional name of the output column.
        """
        self.base = base
        self.exponent = exponent
        if output is not None:
            self.output = output

    def as_expression(self) -> Expr:
        """Return the power operation as a Polars expression.

        Returns:
            A Polars expression representing `base ** exponent`.
        """
        return self.base ** self.exponent  # type: ignore


@register_callback_class
class Root(ExpressionCallback):
    """Compute the signed n-th root of a column.

    The sign of the radicand is preserved, allowing real-valued roots of
    negative numbers for odd indices.
    """

    def __init__(self, radicand: str, index: Real, output: str | None = None) -> None:
        """Initialize the callback.

        Args:
            radicand: Name of the column under the root.
            index: Root index (e.g. 2 for square root).
            root: Optional name of the output column.
        """
        self.radicand = radicand
        self.index = index
        if output is not None:
            self.output = output

    def as_expression(self) -> Expr:
        """Return the root operation as a Polars expression.

        Returns:
            A Polars expression computing the signed n-th root.
        """
        return (self.radicand.sign() * self.radicand.abs() ** (1 / self.index)) # type: ignore
    

@register_callback_class
class Modulo(ExpressionCallback):
    """Compute the modulo (remainder) of two columns or expressions."""

    def __init__(self, dividend: str, divisor: str, output: str | None = None) -> None:
        """Initialize the callback.

        Args:
            dividend: Name of the dividend column.
            divisor: Name of the divisor column.
            remainder: Optional name of the output column.
        """
        self.dividend = dividend
        self.divisor = divisor
        if output is not None:
            self.output = output

    def as_expression(self) -> Expr:
        """Return the modulo operation as a Polars expression.

        Returns:
            A Polars expression representing `dividend % divisor`.
        """
        return self.dividend % self.divisor  # type: ignore