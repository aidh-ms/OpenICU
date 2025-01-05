from open_icu.steps.source.concept import ObservationExtractor
from open_icu.types.conf.concept import Concept, ConceptSource
from open_icu.types.conf.source import SourceConfig


class RAWObservationExtractor(ObservationExtractor):
    pass


class EventObservationExtractor(RAWObservationExtractor):
    def __init__(self, subject_id: str, source: SourceConfig, concept: Concept, concept_source: ConceptSource) -> None:
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
