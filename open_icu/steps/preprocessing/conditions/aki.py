from open_icu.steps.preprocessing.processor.base import Preprocessor
from open_icu.types.base import SubjectData


class AKIPreprocessor(Preprocessor):
    def __init__(self, concepts: list[str]) -> None:
        super().__init__(concepts)

    def process(self, subject_data: SubjectData) -> SubjectData:
        return subject_data
