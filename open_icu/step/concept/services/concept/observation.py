from typing import Annotated, Any, cast

import pandas as pd
from pandas.api.types import is_numeric_dtype
from pandera.typing import DataFrame

from open_icu.step.concept.conf import ConceptConfig
from open_icu.step.concept.services.concept.base import BaseConceptExtractor
from open_icu.type.fhir import FHIRNumericObservation, FHIRTextObservation


class ObservationExtractor(BaseConceptExtractor[FHIRNumericObservation | FHIRTextObservation]):
    """
    A class to extract observation data from a database.

    Parameters
    ----------
    concept_source_config : ConceptSourceConfig
        The concept source configuration.
    args : Any
        The arguments to be passed to the extract method.
    kwargs : Any
        The keyword arguments to be passed to the extract method.
    """

    def _apply_effective_date_time(
        self, df: DataFrame, concept_config: ConceptConfig
    ) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        """
        A method to apply the effective date time to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the effective date time to.
        concept_config : ConceptConfig
            The concept configuration.

        Returns
        -------
        Annotated[pd.DatetimeTZDtype, "ns", "utc"]
            The effective date time.
        """
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["timestamp"], utc=True))

    def _apply_value_quantity__value(self, df: DataFrame, concept_config: ConceptConfig) -> float | str:
        """
        A method to apply the value quantity value to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the value quantity value to.
        concept_config : ConceptConfig
            The concept configuration.

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

    def _apply_value_quantity__unit(self, df: DataFrame, concept_config: ConceptConfig) -> str:
        """
        A method to apply the value quantity unit to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the value quantity unit to.
        concept_config : ConceptConfig
            The concept configuration.

        Returns
        -------
        str
            The value quantity unit.
        """
        return self._concept_source_config.unit[FHIRNumericObservation.value_quantity__unit.replace("__unit", "")]

    def __call__(  # type: ignore[override]
        self, concept_config: ConceptConfig, *args: Any, **kwargs: Any
    ) -> DataFrame[FHIRNumericObservation] | DataFrame[FHIRTextObservation] | None:
        """
        A method to extract observation data from the database.

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
        DataFrame[FHIRNumericObservation] | DataFrame[FHIRTextObservation] | None
            The extracted observation data.
        """
        df: DataFrame = self._get_data()

        if df.empty:
            return None

        observation_df = pd.DataFrame()

        observation_df[FHIRNumericObservation.identifier__coding] = df.apply(
            self._apply_identifier__coding, args=(concept_config,), axis=1
        )
        observation_df[FHIRNumericObservation.subject__reference] = df.apply(
            self._apply_subject__reference, args=(concept_config,), axis=1
        )
        observation_df[FHIRNumericObservation.subject__type] = df.apply(
            self._apply_subject__type, args=(concept_config,), axis=1
        )

        observation_df[FHIRNumericObservation.effective_date_time] = df.apply(
            self._apply_effective_date_time, args=(concept_config,), axis=1
        )
        observation_df[FHIRNumericObservation.value_quantity__value] = df.apply(
            self._apply_value_quantity__value, args=(concept_config,), axis=1
        )
        observation_df[FHIRNumericObservation.value_quantity__unit] = df.apply(
            self._apply_value_quantity__unit, args=(concept_config,), axis=1
        )

        if is_numeric_dtype(observation_df[FHIRNumericObservation.value_quantity__value]):
            return observation_df.pipe(DataFrame[FHIRNumericObservation])

        return observation_df.pipe(DataFrame[FHIRTextObservation])
