from typing import Any
from abc import ABCMeta, abstractmethod


class Representable(metaclass=ABCMeta):
    @abstractmethod
    def to_dict(self) -> dict[str, Any]: ...

    @abstractmethod
    def to_summary(self) -> dict[str, Any]: ...

    def __str__(self) -> str:
        return str(self.to_summary())

    def __repr__(self) -> str:
        return repr(self.to_dict())
