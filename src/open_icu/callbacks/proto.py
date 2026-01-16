from typing import Any, Protocol, runtime_checkable

from polars import Expr, LazyFrame


@runtime_checkable
class CallbackProtocol(Protocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        ...
