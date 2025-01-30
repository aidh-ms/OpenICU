from open_icu.steps.sink.base import SinkStep
from open_icu.types.base import SubjectData


class JSONSinkStep(SinkStep):
    """
    A sink step that writes the data to a JSONL files.

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
        A method that writes the data to a JSONL file per concept.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to write.

        Returns
        -------
        SubjectData
            The subject data that was written.
        """
        # TODO: Implement JSONSinkStep
        return subject_data
