from open_icu.steps.sink.base import SinkStep
from open_icu.types.base import SubjectData


class CSVSinkStep(SinkStep):
    """
    A sink step that writes the data to a CSV files.

    Parameters
    ----------
    sink_path : Path
        The path to the sink directory.
    configs : Path | list[SinkConfig] | None
        The path to the configuration files or a list of configurations.
    concept_configs : Path | list[ConceptConfig] | None
        The path to the concept configuration files or a list of configurations.
    parent : BaseStep | None
        The parent step.
    """

    def process(self, subject_data: SubjectData) -> SubjectData:
        """
        A method that writes the data to a CSV file per concept.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to write.

        Returns
        -------
        SubjectData
            The subject data that was written.
        """
        if not self._sink_path.exists():
            self._sink_path.mkdir(parents=True)

        for concept_name, concept_data in subject_data.data.items():
            concept_path = self._sink_path / f"{concept_name}.csv"

            concept_data.to_csv(concept_path, mode="a+", index=False, header=not concept_path.exists())

        return subject_data
