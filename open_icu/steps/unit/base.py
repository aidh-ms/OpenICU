from open_icu.steps.base import BaseStep
from open_icu.types.base import SubjectData


class UnitConversionStep(BaseStep):
    def process(self, subject_data: SubjectData) -> SubjectData:
        return super().process(subject_data)
