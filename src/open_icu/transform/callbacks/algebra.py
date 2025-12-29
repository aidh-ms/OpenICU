import operator
from numbers import Real

import polars as pl
from polars import Expr, LazyFrame

from open_icu.transform.callbacks.proto import HybridCallback
from open_icu.transform.callbacks.registry import register_callback_class


@register_callback_class
class Add(HybridCallback):
    """Add two columns or expressions.

    This hybrid callback supports both expression-level usage (e.g. inside
    an AST-expression) and frame-level usage, where the result is materialized
    as a new column.
    """

    def __init__(self, augend: str, addend: str, sum: str | None = None) -> None:
        """Initialize the callback.

        Args:
            augend: Name of the first operand column.
            addend: Name of the second operand column.
            sum: Optional name of the output column.
        """
        self.augend = augend
        self.addend = addend
        if sum is not None:
            self.result = sum

    def as_expression(self) -> Expr:
        """Return the addition as a Polars expression.

        Returns:
            A Polars expression representing `augend + addend`.
        """
        return self.augend + self.addend  # type: ignore

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        """Apply the addition and add the result as a new column.

        Args:
            lf: Input LazyFrame.

        Returns:
            A LazyFrame with the computed sum column added.
        """
        return lf.with_columns(
            (pl.col(self.augend) + pl.col(self.addend)).alias(self.result)
        )


@register_callback_class
class Sum(HybridCallback):
    """Compute the row-wise sum across multiple columns."""

    def __init__(self, summands: list[str], sum: str | None = None) -> None:
        """Initialize the callback.

        Args:
            summands: List of column names to be summed row-wise.
            sum: Optional name of the output column.
        """
        self.summands = summands
        if sum is not None:
            self.result = sum

    def as_expression(self) -> Expr:
        """Return the horizontal sum as a Polars expression.

        Returns:
            A Polars expression computing the row-wise sum.
        """
        return pl.sum_horizontal([pl.col(c) for c in self.summands])

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        """Apply the row-wise sum and add the result as a new column.

        Args:
            lf: Input LazyFrame.

        Returns:
            A LazyFrame with the computed sum column added.
        """
        return lf.with_columns(
            pl.sum_horizontal([pl.col(c) for c in self.summands]).alias(self.result)
        )


@register_callback_class
class Subtract(HybridCallback):
    """Subtract one column or expression from another."""

    def __init__(self, minuend: str, subtrahend: str, difference: str | None = None) -> None:
        """Initialize the callback.

        Args:
            minuend: Name of the column to subtract from.
            subtrahend: Name of the column to subtract.
            difference: Optional name of the output column.
        """
        self.minuend = minuend
        self.subtrahend = subtrahend
        if difference is not None:
            self.result = difference

    def as_expression(self) -> Expr:
        """Return the subtraction as a Polars expression.

        Returns:
            A Polars expression representing `minuend - subtrahend`.
        """
        return self.minuend - self.subtrahend  # type: ignore

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        """Apply the subtraction and add the result as a new column.

        Args:
            lf: Input LazyFrame.

        Returns:
            A LazyFrame with the computed difference column added.
        """
        return lf.with_columns(
            (pl.col(self.minuend) - pl.col(self.subtrahend)).alias(self.result)
        )


@register_callback_class
class Multiply(HybridCallback):
    """Multiply two columns or expressions."""

    def __init__(self, multiplicand: str, multiplier: str, product: str | None = None) -> None:
        """Initialize the callback.

        Args:
            multiplicand: Name of the first operand column.
            multiplier: Name of the second operand column.
            product: Optional name of the output column.
        """
        self.multiplicand = multiplicand
        self.multiplier = multiplier
        if product is not None:
            self.result = product

    def as_expression(self) -> Expr:
        """Return the multiplication as a Polars expression.

        Returns:
            A Polars expression representing `multiplicand * multiplier`.
        """
        return self.multiplicand * self.multiplier  # type: ignore

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        """Apply the multiplication and add the result as a new column.

        Args:
            lf: Input LazyFrame.

        Returns:
            A LazyFrame with the computed product column added.
        """
        return lf.with_columns(
            (pl.col(self.multiplicand) * pl.col(self.multiplier)).alias(self.result)
        )


@register_callback_class
class Product(HybridCallback):
    """Compute the row-wise product across multiple columns.

    Note:
        This callback is intended for expression-level usage. Frame-level
        materialization is currently not implemented.
    """

    def __init__(self, factor: list[str], product: str | None = None) -> None:
        """Initialize the callback.

        Args:
            factor: List of column names whose values are multiplied row-wise.
            product: Optional name of the output column.
        """
        self.factor = factor
        if product is not None:
            self.result = product

    def as_expression(self) -> Expr:
        """Return the horizontal product as a Polars expression.

        Returns:
            A Polars expression computing the row-wise product.
        """
        return pl.fold(
            acc=pl.lit(1),
            function=operator.mul,
            exprs=[pl.col(c) for c in self.factor],
        )


@register_callback_class
class Divide(HybridCallback):
    """Divide one column or expression by another."""

    def __init__(self, dividend: str, divisor: str, quotient: str | None = None) -> None:
        """Initialize the callback.

        Args:
            dividend: Name of the numerator column.
            divisor: Name of the denominator column.
            quotient: Optional name of the output column.
        """
        self.dividend = dividend
        self.divisor = divisor
        if quotient is not None:
            self.result = quotient

    def as_expression(self) -> Expr:
        """Return the division as a Polars expression.

        Returns:
            A Polars expression representing `dividend / divisor`.
        """
        return self.dividend / self.divisor  # type: ignore

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        """Apply the division and add the result as a new column.

        Args:
            lf: Input LazyFrame.

        Returns:
            A LazyFrame with the computed quotient column added.
        """
        return lf.with_columns(
            (pl.col(self.dividend) / pl.col(self.divisor)).alias(self.result)
        )


@register_callback_class
class Pow(HybridCallback):
    """Raise a column or expression to a power."""

    def __init__(self, base: str, exponent: str, power: str | None = None) -> None:
        """Initialize the callback.

        Args:
            base: Name of the base column.
            exponent: Name of the exponent column.
            power: Optional name of the output column.
        """
        self.base = base
        self.exponent = exponent
        if power is not None:
            self.result = power

    def as_expression(self) -> Expr:
        """Return the power operation as a Polars expression.

        Returns:
            A Polars expression representing `base ** exponent`.
        """
        return self.base ** self.exponent  # type: ignore

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        """Apply the power operation and add the result as a new column.

        Args:
            lf: Input LazyFrame.

        Returns:
            A LazyFrame with the computed power column added.
        """
        return lf.with_columns(
            (pl.col(self.base) ** pl.col(self.exponent)).alias(self.result)
        )


@register_callback_class
class Root(HybridCallback):
    """Compute the signed n-th root of a column.

    The sign of the radicand is preserved, allowing real-valued roots of
    negative numbers for odd indices.
    """

    def __init__(self, radicand: str, index: Real, root: str | None = None) -> None:
        """Initialize the callback.

        Args:
            radicand: Name of the column under the root.
            index: Root index (e.g. 2 for square root).
            root: Optional name of the output column.
        """
        self.radicand = radicand
        self.index = index
        if root is not None:
            self.result = root

    def as_expression(self) -> Expr:
        """Return the root operation as a Polars expression.

        Returns:
            A Polars expression computing the signed n-th root.
        """
        return (
            self.radicand.sign()                                # type: ignore
            * self.radicand.abs() ** (1 / float(self.index))    # type: ignore
        )

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        """Apply the root operation and add the result as a new column.

        Args:
            lf: Input LazyFrame.

        Returns:
            A LazyFrame with the computed root column added.
        """
        return lf.with_columns(
            (
                pl.col(self.radicand).sign()
                * (pl.col(self.radicand).abs() ** (1 / float(self.index)))
            ).alias(self.result)
        )


@register_callback_class
class Modulo(HybridCallback):
    """Compute the modulo (remainder) of two columns or expressions."""

    def __init__(self, dividend: str, divisor: str, remainder: str | None = None) -> None:
        """Initialize the callback.

        Args:
            dividend: Name of the dividend column.
            divisor: Name of the divisor column.
            remainder: Optional name of the output column.
        """
        self.dividend = dividend
        self.divisor = divisor
        if remainder is not None:
            self.result = remainder

    def as_expression(self) -> Expr:
        """Return the modulo operation as a Polars expression.

        Returns:
            A Polars expression representing `dividend % divisor`.
        """
        return self.dividend % self.divisor  # type: ignore

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        """Apply the modulo operation and add the result as a new column.

        Args:
            lf: Input LazyFrame.

        Returns:
            A LazyFrame with the computed remainder column added.
        """
        return lf.with_columns(
            (pl.col(self.dividend) % pl.col(self.divisor)).alias(self.result)
        )
