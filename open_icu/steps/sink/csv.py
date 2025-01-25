from open_icu.steps.sink.base import SinkStep
from open_icu.types.base import SubjectData


class CSVSinkStep(SinkStep):
    def process(self, subject_data: SubjectData) -> SubjectData:
        if not self._sink_path.exists():
            self._sink_path.mkdir(parents=True)

        for concept_name, concept_data in subject_data.data.items():
            concept_path = self._sink_path / f"{concept_name}.csv"

            concept_data.to_csv(concept_path, mode="a+", index=False, header=not concept_path.exists())

        return subject_data
