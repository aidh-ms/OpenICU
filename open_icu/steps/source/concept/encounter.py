import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.source.concept.base import ConceptExtractor
from open_icu.steps.source.database import PandasDatabaseMixin
from open_icu.types.fhir import CodeableConcept, FHIREncounter, Period, Reference


class EncounterExtractor(PandasDatabaseMixin, ConceptExtractor[FHIREncounter]):
    def _apply_subject(self, df: DataFrame) -> Reference:
        return Reference(reference=str(df["subject_id"]), type=self._concept_source.source)

    def _apply_actual_period(self, df: DataFrame) -> Period:
        return Period(
            start=pd.to_datetime(df["start_timestamp"], utc=True),  # type: ignore[typeddict-item]
            end=pd.to_datetime(df["stop_timestamp"], utc=True),  # type: ignore[typeddict-item]
        )

    def _apply_care_team(self, df: DataFrame) -> Reference:
        return Reference(reference=str(df["care_team_id"]), type="CareTeam")

    def _apply_type(self, df: DataFrame) -> CodeableConcept:
        return self._get_concept_identifiers()

    def extract(self) -> DataFrame[FHIREncounter] | None:
        df: DataFrame = self.get_query_df(self._source.connection_uri, **self._concept_source.params)

        if df.empty:
            return None

        encounter_df = pd.DataFrame()

        encounter_df[FHIREncounter.type] = df.apply(self._apply_type, axis=1)
        encounter_df[FHIREncounter.subject] = df.apply(self._apply_subject, axis=1)
        encounter_df[FHIREncounter.actual_period] = df.apply(self._apply_actual_period, axis=1)
        encounter_df[FHIREncounter.care_team] = df.apply(self._apply_care_team, axis=1)

        return encounter_df.pipe(DataFrame[FHIREncounter])
