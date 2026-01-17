import operator
from numbers import Real
from typing import Optional, Sequence

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import CallbackResult, CallbackProtocol, AstValue, to_expr
from open_icu.callbacks.registry import register_callback_class


@register_callback_class
class Add(CallbackProtocol):
    def __init__(self, augend: str, addend: str, output: Optional[str] = None) -> None:
        self.augend = augend
        self.addend = addend
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = to_expr(lf, self.augend) + to_expr(lf, self.addend)

        if self.output is None:
            return expr

        return lf.with_columns(expr.alias(self.output))

@register_callback_class
class Sum(CallbackProtocol):
    def __init__(self, *summands: AstValue, output: Optional[str] = None) -> None:
        if len(summands) == 1 and isinstance(summands[0], list):
            self. summands: Sequence[AstValue] = summands[0]
        else:
            self.summands = summands
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        exprs = [to_expr(lf, s) for s in self.summands]
        expr = pl.sum_horizontal(exprs)
        
        if self.output is None:
            return expr
        
        return lf.with_columns(expr.alias(self.output))

@register_callback_class
class Subtract(CallbackProtocol):
    def __init__(self, minuend: str, subtrahend: str, output: Optional[str] = None) -> None:
        self.minuend = minuend
        self.subtrahend = subtrahend
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return lf.with_columns(
            (pl.col(self.minuend) - pl.col(self.subtrahend)).alias(self.output)
        )

@register_callback_class
class Multiply(CallbackProtocol):
    def __init__(self, multiplicand: str, multiplier: str, output: Optional[str] = None) -> None:
        self.multiplicand = multiplicand
        self.multiplier = multiplier
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return lf.with_columns(
            (pl.col(self.multiplicand) * pl.col(self.multiplier)).alias(self.output)
        )

@register_callback_class
class Product(CallbackProtocol):
    def __init__(self, factor: list[str], output: Optional[str] = None) -> None:
        self.factor = factor
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return lf.with_columns(
            (pl.fold(acc=pl.lit(1),function=operator.mul, exprs=pl.col(self.factor)).alias(self.output))
        )

@register_callback_class
class Divide(CallbackProtocol):
    def __init__(self, dividend: str, divisor: str, output: Optional[str] = None) -> None:
        self.dividend = dividend
        self.divisor = divisor
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return lf.with_columns(
            (pl.col(self.dividend) / pl.col(self.divisor)).alias(self.output)
        )

@register_callback_class
class Pow(CallbackProtocol):
    def __init__(self, base: str, exponent: str, output: Optional[str] = None) -> None:
        self.base = base
        self.exponent = exponent
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return lf.with_columns(
            (pl.col(self.base) ** pl.col(self.exponent)).alias(self.output)
        )

@register_callback_class
class Root(CallbackProtocol):
    def __init__(self, radicand: str, index: Real, output: Optional[str] = None) -> None:
        self.radicand = radicand
        self.index = index
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return lf.with_columns(
            (pl.col(self.radicand).sign() * (pl.col(self.radicand).abs() ** (1 / float(self.index))).alias(self.output))
        )

@register_callback_class
class Modulo(CallbackProtocol):
    def __init__(self, dividend: str, divisor: str, output: Optional[str] = None) -> None:
        self.dividend = dividend
        self.divisor = divisor
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return lf.with_columns(
            (pl.col(self.dividend) % pl.col(self.divisor)).alias(self.output)
        )