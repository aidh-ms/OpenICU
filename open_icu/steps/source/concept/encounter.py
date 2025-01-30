from typing import Annotated, cast

import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.source.concept.base import ConceptExtractor
from open_icu.steps.source.database import PandasDatabaseMixin
from open_icu.types.fhir import FHIREncounter


class EncounterExtractor(PandasDatabaseMixin, ConceptExtractor[FHIREncounter]):
    """
    A class to extract encounter data from a database.

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

    def _apply_actual_period__start(self, df: DataFrame) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        """
        A method to apply the actual period start to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the actual period start to.

        Returns
        -------
        Annotated[pd.DatetimeTZDtype, "ns", "utc"]
            The actual period start.
        """
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["start_timestamp"], utc=True))

    def _apply_actual_period__end(self, df: DataFrame) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        """
        A method to apply the actual period end to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the actual period end to.

        Returns
        -------
        Annotated[pd.DatetimeTZDtype, "ns", "utc"]
            The actual period end.
        """
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["stop_timestamp"], utc=True))

    def _apply_care_team(self, df: DataFrame) -> str:
        """
        A method to apply the care team to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the care team to.

        Returns
        -------
        str
            The care team.
        """
        return str(df["care_team_id"])

    def extract(self) -> DataFrame[FHIREncounter] | None:
        """
        A method to extract encounter data from the database.

        Returns
        -------
        DataFrame[FHIREncounter] | None
            The extracted encounter data.
        """
        df: DataFrame = self.get_query_df(self._source.connection_uri, **self._concept_source.params)

        if df.empty:
            return None

        encounter_df = pd.DataFrame()

        encounter_df[FHIREncounter.identifier__coding] = df.apply(self._apply_identifier__coding, axis=1)
        encounter_df[FHIREncounter.subject__reference] = df.apply(self._apply_subject__reference, axis=1)
        encounter_df[FHIREncounter.subject__type] = df.apply(self._apply_subject__type, axis=1)

        encounter_df[FHIREncounter.actual_period__start] = df.apply(self._apply_actual_period__start, axis=1)
        encounter_df[FHIREncounter.actual_period__end] = df.apply(self._apply_actual_period__end, axis=1)
        encounter_df[FHIREncounter.care_team] = df.apply(self._apply_care_team, axis=1)

        return encounter_df.pipe(DataFrame[FHIREncounter])
