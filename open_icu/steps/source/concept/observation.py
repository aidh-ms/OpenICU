from typing import Annotated, cast

import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.source.concept.base import ConceptExtractor
from open_icu.steps.source.database import PandasDatabaseMixin
from open_icu.types.fhir import (
    CodeableConcept,
    FHIRObjectObservation,
    Quantity,
    Reference,
)


class ObservationExtractor(PandasDatabaseMixin, ConceptExtractor[FHIRObjectObservation]):
    def _apply_subject(self, df: DataFrame) -> Reference:
        return Reference(reference=str(df["subject_id"]), type=self._concept_source.source)

    def _apply_effective_date_time(self, df: DataFrame) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["timestamp"], utc=True))

    def _apply_value_quantity(self, df: DataFrame) -> Quantity:
        value = df["value"]
        assert isinstance(value, (int, float, str))
        return Quantity(value=value, unit=self._concept_source.unit["value"])

    def _apply_code(self, df: DataFrame) -> CodeableConcept:
        return self._get_concept_identifiers()

    def extract(self) -> DataFrame[FHIRObjectObservation] | None:
        df: DataFrame = self.get_query_df(self._source.connection_uri, **self._concept_source.params)

        if df.empty:
            return None

        observation_df = pd.DataFrame()

        observation_df[FHIRObjectObservation.subject] = df.apply(self._apply_subject, axis=1)
        observation_df[FHIRObjectObservation.effective_date_time] = df.apply(self._apply_effective_date_time, axis=1)
        observation_df[FHIRObjectObservation.value_quantity] = df.apply(self._apply_value_quantity, axis=1)
        observation_df[FHIRObjectObservation.code] = df.apply(self._apply_code, axis=1)

        return observation_df.pipe(DataFrame[FHIRObjectObservation])
