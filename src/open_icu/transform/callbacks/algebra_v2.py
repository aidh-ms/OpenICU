import polars as pl
from polars import LazyFrame, Expr
from numbers import Real
import operator

from open_icu.transform.callbacks.proto import CallbackProtocol, ExpressionCallback, FrameCallback, HybridCallback
from open_icu.transform.callbacks.registry import register_callback_class


@register_callback_class
class Add(ExpressionCallback):
    def __init__(self, augend: str, addend: str, sum: str) -> None:
        self.augend = augend
        self.addend = addend
        self.sum = sum

    def as_expression(self) -> Expr:
        return (pl.col(self.augend) + pl.col(self.addend)).alias(self.sum)

@register_callback_class
class Sum(ExpressionCallback):
    def __init__(self, summands: list[str], sum: str) -> None:
        self.summands = summands
        self.sum = sum

    def as_expression(self) -> Expr:
        return pl.sum_horizontal([pl.col(c) for c in self.summands]).alias(self.sum)

@register_callback_class
class Subtract(ExpressionCallback):
    def __init__(self, minuend: str, subtrahend: str, difference: str) -> None:
        self.minuend = minuend
        self.subtrahend = subtrahend
        self.difference = difference

    def as_expression(self) -> Expr:
        return (pl.col(self.minuend) - pl.col(self.subtrahend)).alias(self.difference)

@register_callback_class
class Multiply(ExpressionCallback):
    def __init__(self, multiplicand: str, multiplier: str, product: str) -> None:
        self.multiplicand = multiplicand
        self.multiplier = multiplier
        self.product = product

    def as_expression(self) -> Expr:
        return (pl.col(self.multiplicand) * pl.col(self.multiplier)).alias(self.product)

@register_callback_class
class Product(ExpressionCallback):
    def __init__(self, factor: list[str], product: str) -> None:
        self.factor = factor
        self.product = product

    def as_expression(self) -> Expr:
        return (pl.fold(acc=pl.lit(1),function=operator.mul, exprs=pl.col(self.factor)).alias(self.product))

@register_callback_class
class Divide(ExpressionCallback):
    def __init__(self, dividend: str, divisor: str, quotient: str) -> None:
        self.dividend = dividend
        self.divisor = divisor
        self.quotient = quotient

    def as_expression(self) -> Expr:
        return (pl.col(self.dividend) / pl.col(self.divisor)).alias(self.quotient)

@register_callback_class
class Pow(ExpressionCallback):
    def __init__(self, base: str, exponent: str, power: str) -> None:
        self.base = base
        self.exponent = exponent
        self.power = power

    def as_expression(self) -> Expr:
        return (pl.col(self.base) ** pl.col(self.exponent)).alias(self.power)

@register_callback_class
class Root(ExpressionCallback):
    def __init__(self, radicand: str, index: Real, root: str) -> None:
        self.radicand = radicand
        self.index = index
        self.root = root

    def as_expression(self) -> Expr:
        return (pl.col(self.radicand).sign() * (pl.col(self.radicand).abs() ** (1 / float(self.index))).alias(self.root))

@register_callback_class
class Modulo(ExpressionCallback):
    def __init__(self, dividend: str, divisor: str, remainder: str) -> None:
        self.dividend = dividend
        self.divisor = divisor
        self.remainder = remainder
    
    def as_expression(self) -> Expr:
        return (pl.col(self.dividend) % pl.col(self.divisor)).alias(self.remainder)