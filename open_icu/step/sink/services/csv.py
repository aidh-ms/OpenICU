from open_icu.step.sink.base import BaseSinkStep
from open_icu.type.subject import SubjectData


class CSVSinkStep(BaseSinkStep):
    """
    A sink step that writes the data to a CSV files.

    Parameters
    ----------
    sink_path : Path
        The path to the sink directory.
    fail_silently : bool, default: False
        Whether to fail silently or not.
    parent : BaseStep | None
        The parent step.
    """

    def process(self, subject_data: SubjectData) -> SubjectData:
        if not self._sink_path.exists():
            self._sink_path.mkdir(parents=True)

        for concept_name, concept_data in subject_data.data.items():
            concept_path = self._sink_path / f"{concept_name}.csv"

            concept_data.to_csv(concept_path, mode="a+", index=False, header=(not concept_path.exists()))

        return subject_data
