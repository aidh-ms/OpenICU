from open_icu.steps.source.concept import ObservationExtractor
from open_icu.types.conf.concept import Concept, ConceptSource


class RAWObservationExtractor(ObservationExtractor):
    pass


class EventObservationExtractor(ObservationExtractor):
    def __init__(self, connection_uri: str, subject_id: str, concept: Concept, source: ConceptSource) -> None:
        super().__init__(connection_uri, subject_id, concept, source)

        if not isinstance(self._source.params.get("itemid"), list):
            self._source.params["itemid"] = [self._source.params["itemid"]]

        self._source.params["sql"] = """
            SELECT
                subject_id, valuenum as value, charttime as timestamp
            FROM {table}
            WHERE
                subject_id = {subject_id}
                AND itemid = ANY(ARRAY{itemid})
        """
