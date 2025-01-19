from typing import Annotated, cast

import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.source.concept.base import ConceptExtractor
from open_icu.steps.source.database import PandasDatabaseMixin
from open_icu.types.fhir import FHIRMedicationStatement


class MedicationExtractor(PandasDatabaseMixin, ConceptExtractor[FHIRMedicationStatement]):
    def _apply_effective_period_start(self, df: DataFrame) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["start_timestamp"], utc=True))

    def _apply_effective_period_end(self, df: DataFrame) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["stop_timestamp"], utc=True))

    def _apply_dosage__dose_quantity__value(self, df: DataFrame) -> float:
        return float(df["dose"])

    def _apply_doage__dose_quantity__unit(self, df: DataFrame) -> str:
        return self._concept_source.unit[FHIRMedicationStatement.dosage__dose_quantity__unit.replace("__unit", "")]

    def _apply_dosage__rate_quantity__value(self, df: DataFrame) -> float:
        return float(df["rate"])

    def _apply_dosage__rate_quantity__unit(self, df: DataFrame) -> str:
        return self._concept_source.unit[FHIRMedicationStatement.dosage__rate_quantity__unit.replace("__unit", "")]

    def extract(self) -> DataFrame[FHIRMedicationStatement] | None:
        df: DataFrame = self.get_query_df(self._source.connection_uri, **self._concept_source.params)

        if df.empty:
            return None

        medication_df = pd.DataFrame()

        medication_df[FHIRMedicationStatement.identifier__coding] = df.apply(self._apply_identifier__coding, axis=1)
        medication_df[FHIRMedicationStatement.subject__reference] = df.apply(self._apply_subject__reference, axis=1)
        medication_df[FHIRMedicationStatement.subject__type] = df.apply(self._apply_subject__type, axis=1)

        medication_df[FHIRMedicationStatement.effective_period__start] = df.apply(
            self._apply_effective_period_start, axis=1
        )
        medication_df[FHIRMedicationStatement.effective_period__end] = df.apply(
            self._apply_effective_period_end, axis=1
        )
        medication_df[FHIRMedicationStatement.dosage__dose_quantity__value] = df.apply(
            self._apply_dosage__dose_quantity__value, axis=1
        )
        medication_df[FHIRMedicationStatement.dosage__rate_quantity__unit] = df.apply(
            self._apply_dosage__rate_quantity__unit, axis=1
        )
        medication_df[FHIRMedicationStatement.dosage__rate_quantity__value] = df.apply(
            self._apply_dosage__rate_quantity__value, axis=1
        )
        medication_df[FHIRMedicationStatement.dosage__rate_quantity__unit] = df.apply(
            self._apply_dosage__rate_quantity__unit, axis=1
        )

        return medication_df.pipe(DataFrame[FHIRMedicationStatement])
