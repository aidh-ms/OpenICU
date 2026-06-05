from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_expr
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class And(CallbackProtocol):
    def __init__(self, left: AstValue, right: AstValue) -> None:
        self.left = left
        self.right = right

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return to_expr(lf, self.left) & to_expr(lf, self.right)


@register_callback_cls
class Or(CallbackProtocol):
    def __init__(self, left: AstValue, right: AstValue) -> None:
        self.left = left
        self.right = right

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return to_expr(lf, self.left) | to_expr(lf, self.right)


@register_callback_cls
class Not(CallbackProtocol):
    def __init__(self, value: AstValue) -> None:
        self.value = value

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        return ~to_expr(lf, self.value)