import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.source.concept.base import ConceptExtractor
from open_icu.steps.source.database import PandasDatabaseMixin
from open_icu.types.fhir import (
    CodeableConcept,
    CodeableReference,
    Coding,
    Dosage,
    FHIRMedicationStatement,
    Period,
    Quantity,
    Reference,
)


class MedicationExtractor(PandasDatabaseMixin, ConceptExtractor[FHIRMedicationStatement]):
    def _apply_subject(self, df: DataFrame) -> Reference:
        return Reference(reference=str(df["subject_id"]), type=self._concept_source.source)

    def _apply_medication(self, df: DataFrame) -> CodeableReference:
        return CodeableReference(
            concept=CodeableConcept(
                coding=[
                    Coding(code=str(concept_id), system=concept_type)
                    for concept_type, concept_id in self._concept.identifiers.items()
                ]
            )
        )

    def _apply_dosage(self, df: DataFrame) -> Dosage:
        return Dosage(
            dose_quantity=Quantity(value=float(df["dose"]), unit=self._concept_source.unit["dose"]),
            rate_quantity=Quantity(value=float(df["rate"]), unit=self._concept_source.unit["rate"]),
        )

    def _apply_effective_period(self, df: DataFrame) -> Period:
        return Period(
            start=pd.to_datetime(df["start_timestamp"], utc=True),  # type: ignore[typeddict-item]
            end=pd.to_datetime(df["stop_timestamp"], utc=True),  # type: ignore[typeddict-item]
        )

    def extract(self) -> DataFrame[FHIRMedicationStatement] | None:
        df: DataFrame = self.get_query_df(self._source.connection_uri, **self._concept_source.params)

        if df.empty:
            return None

        medication_df = pd.DataFrame()

        medication_df[FHIRMedicationStatement.subject] = df.apply(self._apply_subject, axis=1)
        medication_df[FHIRMedicationStatement.medication] = df.apply(self._apply_medication, axis=1)
        medication_df[FHIRMedicationStatement.dosage] = df.apply(self._apply_dosage, axis=1)
        medication_df[FHIRMedicationStatement.effective_period] = df.apply(self._apply_effective_period, axis=1)

        return medication_df.pipe(DataFrame[FHIRMedicationStatement])
