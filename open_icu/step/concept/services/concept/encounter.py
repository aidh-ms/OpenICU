from typing import Annotated, Any, cast

import pandas as pd
from pandera.typing import DataFrame

from open_icu.step.concept.conf import ConceptConfig
from open_icu.step.concept.services.concept.base import BaseConceptExtractor
from open_icu.type.fhir import FHIREncounter


class EncounterExtractor(BaseConceptExtractor[FHIREncounter]):
    """
    A class to extract encounter data from a database.

    Parameters
    ----------
    concept_source_config : ConceptSourceConfig
        The concept source configuration.
    args : Any
        The arguments to be passed to the extract method.
    kwargs : Any
        The keyword arguments to be passed to the extract method.
    """

    def _apply_actual_period__start(
        self, df: DataFrame, concept_config: ConceptConfig
    ) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        """
        A method to apply the actual period start to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the actual period start to.
        concept_config : ConceptConfig
            The concept configuration.

        Returns
        -------
        Annotated[pd.DatetimeTZDtype, "ns", "utc"]
            The actual period start.
        """
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["start_timestamp"], utc=True))

    def _apply_actual_period__end(
        self, df: DataFrame, concept_config: ConceptConfig
    ) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        """
        A method to apply the actual period end to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the actual period end to.
        concept_config : ConceptConfig
            The concept configuration.

        Returns
        -------
        Annotated[pd.DatetimeTZDtype, "ns", "utc"]
            The actual period end.
        """
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["stop_timestamp"], utc=True))

    def _apply_care_team(self, df: DataFrame, concept_config: ConceptConfig) -> str:
        """
        A method to apply the care team to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the care team to.
        concept_config : ConceptConfig
            The concept configuration.

        Returns
        -------
        str
            The care team.
        """
        return str(df["care_team_id"])

    def extract(self, concept_config: ConceptConfig, *args: Any, **kwargs: Any) -> DataFrame[FHIREncounter] | None:
        """
        A method to extract encounter data from the database.

        Parameters
        ----------
        concept_config : ConceptConfig
            The concept configuration.
        args : Any
            The arguments to be passed to the extract method.
        kwargs : Any
            The keyword arguments to be passed to the extract method.

        Returns
        -------
        DataFrame[FHIREncounter] | None
            The extracted encounter data.
        """
        df: DataFrame = self._get_data()

        if df.empty:
            return None

        encounter_df = pd.DataFrame()

        encounter_df[FHIREncounter.identifier__coding] = df.apply(
            self._apply_identifier__coding, args=(concept_config,), axis=1
        )
        encounter_df[FHIREncounter.subject__reference] = df.apply(
            self._apply_subject__reference, args=(concept_config,), axis=1
        )
        encounter_df[FHIREncounter.subject__type] = df.apply(self._apply_subject__type, args=(concept_config,), axis=1)

        encounter_df[FHIREncounter.actual_period__start] = df.apply(
            self._apply_actual_period__start, args=(concept_config,), axis=1
        )
        encounter_df[FHIREncounter.actual_period__end] = df.apply(
            self._apply_actual_period__end, args=(concept_config,), axis=1
        )
        encounter_df[FHIREncounter.care_team] = df.apply(self._apply_care_team, args=(concept_config,), axis=1)

        return encounter_df.pipe(DataFrame[FHIREncounter])
