from abc import ABC, abstractmethod

from open_icu.types.base import SubjectData


class Preprocessor(ABC):
    def __init__(self, concepts: list[str]) -> None:
        super().__init__()

        self.concepts = concepts

    def __call__(self, subject_data: SubjectData) -> SubjectData:
        return self.process(subject_data)

    @abstractmethod
    def process(self, subject_data: SubjectData) -> SubjectData:
        raise NotImplementedError
