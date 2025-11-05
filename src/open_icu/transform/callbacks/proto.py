from typing import Any, Protocol

from polars import LazyFrame


class CallbackProtocol(Protocol):
    def __init__(self, **kwargs: Any) -> None:
        ...

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        ...
