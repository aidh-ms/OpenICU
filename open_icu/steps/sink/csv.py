from open_icu.steps.base import BaseStep
from open_icu.types.base import SubjectData


class CSVSinkStep(BaseStep):
    def process(self, subject_data: SubjectData) -> SubjectData:
        return subject_data
