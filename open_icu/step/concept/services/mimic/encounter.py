from typing import Any

from open_icu.step.concept.conf import ConceptSourceConfig
from open_icu.step.concept.services.concept import EncounterExtractor as RAWEncounterExtractor


class ICUEncounterExtractor(RAWEncounterExtractor):
    """
    A base class for extracting data from a source based on a concept.

    Parameters
    ----------
    concept_source_config : ConceptSourceConfig
        The concept source configuration.
    args : Any
        The arguments to be passed to the extract method.
    kwargs : Any
        The keyword arguments to be passed to the extract method.
    """

    def __init__(self, concept_source_config: ConceptSourceConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(concept_source_config, *args, **kwargs)

        self._concept_source_config.kwargs["query"] = """
            SELECT
                subject_id,
                intime as start_timestamp,
                outtime as stop_timestamp,
                first_careunit as care_team_id
            FROM mimiciv_icu.icustays
            WHERE
                subject_id = {subject_id}
        """
