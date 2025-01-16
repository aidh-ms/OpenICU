from open_icu.steps.base import BaseStep
from open_icu.types.base import SubjectData


class SinkStep(BaseStep):
    def process(self, subject_data: SubjectData) -> SubjectData:
        return super().process(subject_data)
