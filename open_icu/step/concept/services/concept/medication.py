from typing import Annotated, Any, cast

import pandas as pd
from pandera.typing import DataFrame

from open_icu.step.concept.conf import ConceptConfig
from open_icu.step.concept.services.concept.base import BaseConceptExtractor
from open_icu.type.fhir import FHIRMedicationStatement


class MedicationExtractor(BaseConceptExtractor[FHIRMedicationStatement]):
    """
    A class to extract medication data from a database.

    Parameters
    ----------
    concept_source_config : ConceptSourceConfig
        The concept source configuration.
    args : Any
        The arguments to be passed to the extract method.
    kwargs : Any
        The keyword arguments to be passed to the extract method.
    """

    def _apply_effective_period_start(
        self, df: DataFrame, concept_config: ConceptConfig
    ) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        """
        A method to apply the effective period start to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the effective period start to.

        Returns
        -------
        Annotated[pd.DatetimeTZDtype, "ns", "utc"]
            The effective period start.
        """
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["start_timestamp"], utc=True))

    def _apply_effective_period_end(
        self, df: DataFrame, concept_config: ConceptConfig
    ) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        """
        A method to apply the effective period end to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the effective period end to.

        Returns
        -------
        Annotated[pd.DatetimeTZDtype, "ns", "utc"]
            The effective period end.
        """
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["stop_timestamp"], utc=True))

    def _apply_dosage__dose_quantity__value(self, df: DataFrame, concept_config: ConceptConfig) -> float:
        """
        A method to apply the dosage dose quantity value to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the dosage dose quantity value to.

        Returns
        -------
        float
            The dosage dose quantity value.
        """
        return float(df["dose"])

    def _apply_doage__dose_quantity__unit(self, df: DataFrame, concept_config: ConceptConfig) -> str:
        """
        A method to apply the dosage dose quantity unit to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the dosage dose quantity unit to.

        Returns
        -------
        str
            The dosage dose quantity unit.
        """
        return self._concept_source_config.unit[
            FHIRMedicationStatement.dosage__dose_quantity__unit.replace("__unit", "")
        ]

    def _apply_dosage__rate_quantity__value(self, df: DataFrame, concept_config: ConceptConfig) -> float:
        """
        A method to apply the dosage rate quantity value to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the dosage rate quantity value to.

        Returns
        -------
        float
            The dosage rate quantity value.
        """
        return float(df["rate"])

    def _apply_dosage__rate_quantity__unit(self, df: DataFrame, concept_config: ConceptConfig) -> str:
        """
        A method to apply the dosage rate quantity unit to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the dosage rate quantity unit to.

        Returns
        -------
        str
            The dosage rate quantity unit.
        """
        return self._concept_source_config.unit[
            FHIRMedicationStatement.dosage__rate_quantity__unit.replace("__unit", "")
        ]

    def __call__(
        self, concept_config: ConceptConfig, subject_id: str, *args: Any, **kwargs: Any
    ) -> DataFrame[FHIRMedicationStatement] | None:
        """
        A method to extract medication data from the database.

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
        DataFrame[FHIRMedicationStatement] | None
            The extracted medication data.
        """
        df: DataFrame = self._get_data(subject_id)

        if df.empty:
            return None

        medication_df = pd.DataFrame()

        medication_df[FHIRMedicationStatement.identifier__coding] = df.apply(
            self._apply_identifier__coding, args=(concept_config,), axis=1
        )
        medication_df[FHIRMedicationStatement.subject__reference] = df.apply(
            self._apply_subject__reference, args=(concept_config,), axis=1
        )
        medication_df[FHIRMedicationStatement.subject__type] = df.apply(
            self._apply_subject__type, args=(concept_config,), axis=1
        )

        medication_df[FHIRMedicationStatement.effective_period__start] = df.apply(
            self._apply_effective_period_start, args=(concept_config,), axis=1
        )
        medication_df[FHIRMedicationStatement.effective_period__end] = df.apply(
            self._apply_effective_period_end, args=(concept_config,), axis=1
        )
        medication_df[FHIRMedicationStatement.dosage__dose_quantity__value] = df.apply(
            self._apply_dosage__dose_quantity__value, args=(concept_config,), axis=1
        )
        medication_df[FHIRMedicationStatement.dosage__rate_quantity__unit] = df.apply(
            self._apply_dosage__rate_quantity__unit, args=(concept_config,), axis=1
        )
        medication_df[FHIRMedicationStatement.dosage__rate_quantity__value] = df.apply(
            self._apply_dosage__rate_quantity__value, args=(concept_config,), axis=1
        )
        medication_df[FHIRMedicationStatement.dosage__rate_quantity__unit] = df.apply(
            self._apply_dosage__rate_quantity__unit, args=(concept_config,), axis=1
        )

        return medication_df.pipe(DataFrame[FHIRMedicationStatement])
