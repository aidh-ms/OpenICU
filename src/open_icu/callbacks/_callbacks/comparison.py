from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_expr
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class GreaterThan(CallbackProtocol):
    def __init__(self, left: AstValue, right: AstValue) -> None:
        self.left = left
        self.right = right

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return to_expr(lf, self.left) > to_expr(lf, self.right)


@register_callback_cls
class LessThan(CallbackProtocol):
    def __init__(self, left: AstValue, right: AstValue) -> None:
        self.left = left
        self.right = right

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return to_expr(lf, self.left) < to_expr(lf, self.right)


@register_callback_cls
class GreaterEqual(CallbackProtocol):
    def __init__(self, left: AstValue, right: AstValue) -> None:
        self.left = left
        self.right = right

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return to_expr(lf, self.left) >= to_expr(lf, self.right)


@register_callback_cls
class LessEqual(CallbackProtocol):
    def __init__(self, left: AstValue, right: AstValue) -> None:
        self.left = left
        self.right = right

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return to_expr(lf, self.left) <= to_expr(lf, self.right)


@register_callback_cls
class Equal(CallbackProtocol):
    def __init__(self, left: AstValue, right: AstValue) -> None:
        self.left = left
        self.right = right

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return to_expr(lf, self.left) == to_expr(lf, self.right)


@register_callback_cls
class NotEqual(CallbackProtocol):
    def __init__(self, left: AstValue, right: AstValue) -> None:
        self.left = left
        self.right = right

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return to_expr(lf, self.left) != to_expr(lf, self.right)
