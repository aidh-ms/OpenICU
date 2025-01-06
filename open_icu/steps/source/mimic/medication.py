from open_icu.steps.source.concept import MedicationExtractor
from open_icu.types.conf.concept import Concept, ConceptSource
from open_icu.types.conf.source import SourceConfig


class RAWMedicationExtractor(MedicationExtractor):
    pass


class EventMedicationExtractor(RAWMedicationExtractor):
    def __init__(self, subject_id: str, source: SourceConfig, concept: Concept, concept_source: ConceptSource) -> None:
        super().__init__(subject_id, source, concept, concept_source)

        if not isinstance(self._concept_source.params.get("itemid"), list):
            self._concept_source.params["itemid"] = [self._concept_source.params["itemid"]]

        self._concept_source.params["sql"] = """
            SELECT
                subject_id,
                amount as dose,
                rate,
                starttime as start_timestamp,
                endtime as stop_timestamp
            FROM {table}
            WHERE
                subject_id = {subject_id}
                AND itemid = ANY(ARRAY{itemid})
        """


class EventPerWeightMedicationExtractor(EventMedicationExtractor):
    def __init__(self, subject_id: str, source: SourceConfig, concept: Concept, concept_source: ConceptSource) -> None:
        super().__init__(subject_id, source, concept, concept_source)

        self._concept_source.params["sql"] = """
            SELECT
                subject_id,
                amount as dose,
                (rate * patientweight) as rate,
                starttime as start_timestamp,
                endtime as stop_timestamp
            FROM {table}
            WHERE
                subject_id = {subject_id}
                AND itemid = ANY(ARRAY{itemid})
        """
