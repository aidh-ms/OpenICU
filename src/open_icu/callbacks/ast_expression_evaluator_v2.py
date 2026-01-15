
from typing import Optional

from open_icu.callbacks.proto import CallbackProtocol
from open_icu.callbacks.registry import CallbackRegistry, register_callback_class


@register_callback_class
class AbstractSyntaxTree(CallbackProtocol):
    def __init__(self, expr: str, output: Optional[str]) -> None:
        self.expr = expr
        self.output = output
        self.registry = CallbackRegistry()