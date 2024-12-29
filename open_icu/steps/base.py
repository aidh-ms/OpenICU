from abc import ABC


class BaseStep(ABC):
    def pre_process(self) -> None:
        pass

    def process(self) -> None:
        pass

    def post_process(self) -> None:
        pass

    def filter(self) -> None:
        pass

    def validate(self) -> None:
        pass
