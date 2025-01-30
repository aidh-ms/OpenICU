from open_icu.steps.source.concept import ObservationExtractor
from open_icu.types.conf.concept import ConceptConfig, ConceptSource
from open_icu.types.conf.source import SourceConfig


class RAWObservationExtractor(ObservationExtractor):
    """
    A class to extract observation data from a mimic database with a raw sql query.

    Parameters
    ----------
    subject_id : str
        The subject ID to extract data for.
    source : SourceConfig
        The source configuration.
    concept : ConceptConfig
        The concept configuration.
    concept_source : ConceptSource
        The concept source configuration.
    """


class EventObservationExtractor(RAWObservationExtractor):
    """
    A class to extract observation data from a mimic database.

    Parameters
    ----------
    subject_id : str
        The subject ID to extract data for.
    source : SourceConfig
        The source configuration.
    concept : ConceptConfig
        The concept configuration.
    concept_source : ConceptSource
        The concept source configuration.
    """

    def __init__(
        self, subject_id: str, source: SourceConfig, concept: ConceptConfig, concept_source: ConceptSource
    ) -> None:
        super().__init__(subject_id, source, concept, concept_source)

        if not isinstance(self._concept_source.params.get("itemid"), list):
            self._concept_source.params["itemid"] = [self._concept_source.params["itemid"]]

        self._concept_source.params["sql"] = """
            SELECT
                subject_id,
                valuenum as value,
                charttime as timestamp
            FROM {table}
            WHERE
                subject_id = {subject_id}
                AND itemid = ANY(ARRAY{itemid})
        """
