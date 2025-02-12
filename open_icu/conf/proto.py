from typing import Any, Protocol


class ServiceProto(Protocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        ...
