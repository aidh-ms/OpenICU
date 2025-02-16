from typing import Any

from open_icu.step.concept.conf import ConceptSourceConfig
from open_icu.step.concept.services.concept import MedicationExtractor as RAWMedicationExtractor


class EventMedicationExtractor(RAWMedicationExtractor):
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

        if not isinstance(self._concept_source_config.kwargs.get("itemid"), list):
            self._concept_source_config.kwargs["itemid"] = [self._concept_source_config.kwargs["itemid"]]

        self._concept_source_config.kwargs["query"] = """
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
                amount as dose,
                (rate * patientweight) as rate,
                starttime as start_timestamp,
                endtime as stop_timestamp
            FROM {table}
            WHERE
                subject_id = {subject_id}
                AND itemid = ANY(ARRAY{itemid})
        """
