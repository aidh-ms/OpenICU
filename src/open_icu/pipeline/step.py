from abc import ABC, abstractmethod


class Step(ABC):
    @abstractmethod
    def transform(self) -> None:
        pass
