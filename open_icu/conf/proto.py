from typing import Any, Protocol


class ServiceProto(Protocol):
    """
    A protocol for a service class.

    This is used as bound for the ServiceConfiguration class generic type T.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        ...
