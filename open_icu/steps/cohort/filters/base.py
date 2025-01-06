from abc import ABC, abstractmethod

from open_icu.types.base import SubjectData


class CohortFilter(ABC):
    def __init__(self, concepts: list[str]) -> None:
        super().__init__()

        self.concepts = concepts

    def __call__(self, subject_data: SubjectData) -> bool:
        return self.filter(subject_data)

    @abstractmethod
    def filter(self, subject_data: SubjectData) -> bool:
        raise NotImplementedError
