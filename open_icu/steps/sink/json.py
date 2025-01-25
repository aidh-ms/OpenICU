from open_icu.steps.sink.base import SinkStep
from open_icu.types.base import SubjectData


class JSONSinkStep(SinkStep):
    def process(self, subject_data: SubjectData) -> SubjectData:
        return subject_data
