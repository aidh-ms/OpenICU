from typing import Annotated, cast

import pandas as pd
from pandas.api.types import is_numeric_dtype
from pandera.typing import DataFrame

from open_icu.steps.source.concept.base import ConceptExtractor
from open_icu.steps.source.database import PandasDatabaseMixin
from open_icu.types.fhir import FHIRNumericObservation, FHIRTextObservation


class ObservationExtractor(PandasDatabaseMixin, ConceptExtractor[FHIRNumericObservation | FHIRTextObservation]):
    """
    A class to extract observation data from a database.

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

    def _apply_effective_date_time(self, df: DataFrame) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        """
        A method to apply the effective date time to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the effective date time to.

        Returns
        -------
        Annotated[pd.DatetimeTZDtype, "ns", "utc"]
            The effective date time.
        """
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["timestamp"], utc=True))

    def _apply_value_quantity__value(self, df: DataFrame) -> float | str:
        """
        A method to apply the value quantity value to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the value quantity value to.

        Returns
        -------
        float | str
            The value quantity value.
        """
        value = df["value"]
        if isinstance(value, int):
            value = float(value)
        assert isinstance(value, (float, str))
        return value

    def _apply_value_quantity__unit(self, df: DataFrame) -> str:
        """
        A method to apply the value quantity unit to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the value quantity unit to.

        Returns
        -------
        str
            The value quantity unit.
        """
        return self._concept_source.unit[FHIRNumericObservation.value_quantity__unit.replace("__unit", "")]

    def extract(self) -> DataFrame[FHIRNumericObservation] | DataFrame[FHIRTextObservation] | None:  # type: ignore[override]
        """
        A method to extract observation data from the database.

        Returns
        -------
        DataFrame[FHIRNumericObservation] | DataFrame[FHIRTextObservation] | None
            The extracted observation data.
        """
        df: DataFrame = self.get_query_df(self._source.connection_uri, **self._concept_source.params)

        if df.empty:
            return None

        observation_df = pd.DataFrame()

        observation_df[FHIRNumericObservation.identifier__coding] = df.apply(self._apply_identifier__coding, axis=1)
        observation_df[FHIRNumericObservation.subject__reference] = df.apply(self._apply_subject__reference, axis=1)
        observation_df[FHIRNumericObservation.subject__type] = df.apply(self._apply_subject__type, axis=1)

        observation_df[FHIRNumericObservation.effective_date_time] = df.apply(self._apply_effective_date_time, axis=1)
        observation_df[FHIRNumericObservation.value_quantity__value] = df.apply(
            self._apply_value_quantity__value, axis=1
        )
        observation_df[FHIRNumericObservation.value_quantity__unit] = df.apply(self._apply_value_quantity__unit, axis=1)

        if is_numeric_dtype(observation_df[FHIRNumericObservation.value_quantity__value]):
            return observation_df.pipe(DataFrame[FHIRNumericObservation])

        return observation_df.pipe(DataFrame[FHIRTextObservation])
