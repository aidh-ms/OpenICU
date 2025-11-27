import polars as pl
from polars import LazyFrame, Expr
from numbers import Real
import operator

from open_icu.transform.callbacks.proto import CallbackProtocol, HybridCallback, FrameCallback, HybridCallback
from open_icu.transform.callbacks.registry import register_callback_class


@register_callback_class
class Add(HybridCallback):
    def __init__(self, augend: str, addend: str, sum: str | None = None) -> None:
        self.augend = augend
        self.addend = addend
        self.result = sum

    def as_expression(self) -> Expr:
        return (self.augend + self.addend)

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns((pl.col(self.augend) + pl.col(self.addend)).alias(self.result))

#TODO: not working
@register_callback_class
class Sum(HybridCallback):
    def __init__(self, summands: list[str], sum: str | None = None) -> None:
        self.summands = summands
        self.result = sum

    def as_expression(self) -> Expr:
        return pl.sum_horizontal([pl.col(c) for c in self.summands])

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(pl.sum_horizontal([pl.col(c) for c in self.summands]).alias(self.result))
    
@register_callback_class
class Subtract(HybridCallback):
    def __init__(self, minuend: str, subtrahend: str, difference: str | None = None) -> None:
        self.minuend = minuend
        self.subtrahend = subtrahend
        self.result = difference

    def as_expression(self) -> Expr:
        return (self.minuend - self.subtrahend)

    def as_field(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns((pl.col(self.minuend) - pl.col(self.subtrahend)).alias(self.result))

@register_callback_class
class Multiply(HybridCallback):
    def __init__(self, multiplicand: str, multiplier: str, product: str | None = None) -> None:
        self.multiplicand = multiplicand
        self.multiplier = multiplier
        self.result = product

    def as_expression(self) -> Expr:
        return (pl.col(self.multiplicand) * pl.col(self.multiplier))

@register_callback_class
class Product(HybridCallback):
    def __init__(self, factor: list[str], product: str | None = None) -> None:
        self.factor = factor
        self.result = product

    def as_expression(self) -> Expr:
        return (pl.fold(acc=pl.lit(1),function=operator.mul, exprs=pl.col(self.factor)))

@register_callback_class
class Divide(HybridCallback):
    def __init__(self, dividend: str, divisor: str, quotient: str | None = None) -> None:
        self.dividend = dividend
        self.divisor = divisor
        self.result = quotient

    def as_expression(self) -> Expr:
        return (pl.col(self.dividend) / pl.col(self.divisor))

@register_callback_class
class Pow(HybridCallback):
    def __init__(self, base: str, exponent: str, power: str | None = None) -> None:
        self.base = base
        self.exponent = exponent
        self.result = power

    def as_expression(self) -> Expr:
        return (pl.col(self.base) ** pl.col(self.exponent))

@register_callback_class
class Root(HybridCallback):
    def __init__(self, radicand: str, index: Real, root: str | None = None) -> None:
        self.radicand = radicand
        self.index = index
        self.result = root

    def as_expression(self) -> Expr:
        return (pl.col(self.radicand).sign() * (pl.col(self.radicand).abs() ** (1 / float(self.index))))

@register_callback_class
class Modulo(HybridCallback):
    def __init__(self, dividend: str, divisor: str, remainder: str | None = None) -> None:
        self.dividend = dividend
        self.divisor = divisor
        self.result = remainder
    
    def as_expression(self) -> Expr:
        return (pl.col(self.dividend) % pl.col(self.divisor))