import polars as pl
from polars import LazyFrame
from numbers import Number

from open_icu.transform.callbacks.proto import CallbackProtocol
from open_icu.transform.callbacks.registry import register_callback_class


@register_callback_class
class Add(CallbackProtocol):
    def __init__(self, augend: str, addend: str, sum: str) -> None:
        self.augend = augend
        self.addend = addend
        self.sum = sum

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(
            (pl.col(self.augend) + pl.col(self.addend)).alias(self.sum)
        )

@register_callback_class
class Sum(CallbackProtocol):
    def __init__(self, summands: list[str], sum: str) -> None:
        self.summands = summands
        self.sum = sum

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(
            (pl.sum_horizontal([pl.col(c) for c in self.summands]).alias(self.sum))
        )

@register_callback_class
class Subtract(CallbackProtocol):
    def __init__(self, minuend: str, subtrahend: str, difference: str) -> None:
        self.minuend = minuend
        self.subtrahend = subtrahend
        self.difference = difference

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(
            (pl.col(self.minuend) - pl.col(self.subtrahend)).alias(self.difference)
        )

@register_callback_class
class Multiply(CallbackProtocol):
    def __init__(self, multiplicand: str, multiplier: str, product: str) -> None:
        self.multiplicand = multiplicand
        self.multiplier = multiplier
        self.product = product

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(
            (pl.col(self.multiplicand) * pl.col(self.multiplier)).alias(self.product)
        )

@register_callback_class
class Product(CallbackProtocol):
    def __init__(self, factor: list[str], product: str) -> None:
        self.factor = factor
        self.product = product

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(
            (pl.pro_hor([pl.col(c) for c in self.factor]).alias(self.product))
        )

@register_callback_class
class Divide(CallbackProtocol):
    def __init__(self, dividend: str, divisor: str, quotient: str) -> None:
        self.dividend = dividend
        self.divisor = divisor
        self.quotient = quotient

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(
            (pl.col(self.dividend) / pl.col(self.divisor)).alias(self.quotient)
        )

@register_callback_class
class Pow(CallbackProtocol):
    def __init__(self, base: str, exponent: str, power: str) -> None:
        self.base = base
        self.exponent = exponent
        self.power = power

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(
            (pl.col(self.base) ** pl.col(self.exponent)).alias(self.power)
        )

@register_callback_class
class Root(CallbackProtocol):
    def __init__(self, radicand: str, index: Number, root: str) -> None:
        self.radicand = radicand
        self.index = index
        self.root = root

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(
            (pl.col(self.radicand).sign() * (pl.col(self.radicand).abs() ** (1 / pl.col(self.index))).alias(self.root))
        )

@register_callback_class
class Modulo(CallbackProtocol):
    def __init__(self, dividend: str, divisor: str, remainder: str) -> None:
        self.dividend = dividend
        self.divisor = divisor
        self.remainder = remainder

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        return lf.with_columns(
            (pl.col(self.dividend) % pl.col(self.divisor)).alias(self.remainder)
        )