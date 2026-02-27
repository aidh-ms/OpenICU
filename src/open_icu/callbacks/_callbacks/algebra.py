import operator
from typing import Optional, Sequence

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_expr
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class Add(CallbackProtocol):
    def __init__(self, augend: AstValue, addend: AstValue, output: Optional[str] = None) -> None:
        self.augend = augend
        self.addend = addend
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = to_expr(lf, self.augend) + to_expr(lf, self.addend)
        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class Sum(CallbackProtocol):
    def __init__(self, *summands: AstValue, output: Optional[str] = None) -> None:
        if len(summands) == 1 and isinstance(summands[0], list):
            self.summands: Sequence[AstValue] = summands[0]  # type: ignore[invalid-assignment]
        else:
            self.summands = summands
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        exprs = [to_expr(lf, s) for s in self.summands]

        if not exprs:
            expr = pl.lit(0)
            if self.output is None:
                return expr
            return expr.alias(self.output)


        expr = pl.sum_horizontal(exprs)
        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class Subtract(CallbackProtocol):
    def __init__(self, minuend: AstValue, subtrahend: AstValue, output: Optional[str] = None) -> None:
        self.minuend = minuend
        self.subtrahend = subtrahend
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = to_expr(lf, self.minuend) - to_expr(lf, self.subtrahend)
        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class Multiply(CallbackProtocol):
    def __init__(self, multiplicand: AstValue, multiplier: AstValue, output: Optional[str] = None) -> None:
        self.multiplicand = multiplicand
        self.multiplier = multiplier
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = to_expr(lf, self.multiplicand) * to_expr(lf, self.multiplier)
        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class Product(CallbackProtocol):
    def __init__(self, *factors: AstValue, output: Optional[str] = None) -> None:
        if len(factors) == 1 and isinstance(factors[0], list):
            self.factors: Sequence[AstValue] = factors[0]  # type: ignore[invalid-assignment]
        else:
            self.factors = factors
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        exprs = [to_expr(lf, f) for f in self.factors]

        if not exprs:
            expr = pl.lit(1)
            if self.output is None:
                return expr
            return expr.alias(self.output)


        expr = pl.fold(acc=pl.lit(1), function=operator.mul, exprs=exprs)
        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class Divide(CallbackProtocol):
    def __init__(self, dividend: AstValue, divisor: AstValue, output: Optional[str] = None) -> None:
        self.dividend = dividend
        self.divisor = divisor
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = to_expr(lf, self.dividend) / to_expr(lf, self.divisor)
        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class Pow(CallbackProtocol):
    def __init__(self, base: AstValue, exponent: AstValue, output: Optional[str] = None) -> None:
        self.base = base
        self.exponent = exponent
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = to_expr(lf, self.base) ** to_expr(lf, self.exponent)
        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class Root(CallbackProtocol):
    def __init__(self, radicand: AstValue, index: AstValue, output: Optional[str] = None) -> None:
        self.radicand = radicand
        self.index = index
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        radicand_expr = to_expr(lf, self.radicand)
        idx_expr = to_expr(lf, self.index)
        expr = radicand_expr.sign() * radicand_expr.abs() ** (pl.lit(1) / idx_expr)
        if self.output is None:
            return expr
        return expr.alias(self.output)


@register_callback_cls
class Modulo(CallbackProtocol):
    def __init__(self, dividend: AstValue, divisor: AstValue, output: Optional[str] = None) -> None:
        self.dividend = dividend
        self.divisor = divisor
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = to_expr(lf, self.dividend) % to_expr(lf, self.divisor)
        if self.output is None:
            return expr
        return expr.alias(self.output)
