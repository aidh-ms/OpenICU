from open_icu.steps.source.concept import EncounterExtractor
from open_icu.types.conf.concept import ConceptConfig, ConceptSource
from open_icu.types.conf.source import SourceConfig


class RAWEncounterExtractor(EncounterExtractor):
    """
    A class to extract encounter data from a mimic database with a raw sql query.

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


class ICUEncounterExtractor(EncounterExtractor):
    """
    A class to extract encounter data from a mimic database.

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
