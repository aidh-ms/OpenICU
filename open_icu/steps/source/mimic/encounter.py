from open_icu.steps.source.concept import EncounterExtractor
from open_icu.types.conf.concept import Concept, ConceptSource
from open_icu.types.conf.source import SourceConfig


class RAWEncounterExtractor(EncounterExtractor):
    pass


class ICUEncounterExtractor(EncounterExtractor):
    def __init__(self, subject_id: str, source: SourceConfig, concept: Concept, concept_source: ConceptSource) -> None:
        super().__init__(subject_id, source, concept, concept_source)

        self._concept_source.params["sql"] = """
            SELECT
                subject_id,
                intime as start_timestamp,
                outtime as stop_timestamp,
                first_careunit as care_team_id
            FROM mimiciv_icu.icustays
            WHERE
                subject_id = {subject_id}
        """
