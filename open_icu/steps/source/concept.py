from abc import ABC, abstractmethod
from typing import Annotated, Generic, TypeVar, cast

import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.source.database import PandasDatabaseMixin
from open_icu.types.conf.concept import Concept, ConceptSource
from open_icu.types.conf.source import SourceConfig
from open_icu.types.fhir import (
    CodeableConcept,
    CodeableReference,
    Coding,
    Dosage,
    FHIRDeviceUsage,
    FHIREncounter,
    FHIRMedicationStatement,
    FHIRObservation,
    FHIRSchema,
    Period,
    Quantity,
    Reference,
    StatusCodes,
)

F = TypeVar("F", bound=FHIRSchema)


class ConceptExtractor(ABC, Generic[F]):
    def __init__(self, subject_id: str, source: SourceConfig, concept: Concept, concept_source: ConceptSource) -> None:
        self._source = source
        self._concept = concept
        self._concept_source = concept_source

        self._concept_source.params["subject_id"] = subject_id

    def __call__(self) -> DataFrame[F] | None:
        return self.extract()

    @abstractmethod
    def extract(self) -> DataFrame[F] | None:
        raise NotImplementedError


class ObservationExtractor(PandasDatabaseMixin, ConceptExtractor[FHIRObservation]):
    def _apply_subject(self, df: DataFrame) -> Reference:
        return Reference(reference=str(df["subject_id"]), type=self._concept_source.source)

    def _apply_effective_date_time(self, df: DataFrame) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["timestamp"], utc=True))

    def _apply_value_quantity(self, df: DataFrame) -> Quantity:
        value = df["value"]
        assert isinstance(value, (int, float, str))
        return Quantity(value=value, unit=self._concept_source.unit["value"])

    def _apply_code(self, df: DataFrame) -> CodeableConcept:
        return CodeableConcept(
            coding=[
                Coding(code=str(concept_id), system=concept_type)
                for concept_type, concept_id in self._concept.identifiers.items()
            ]
        )

    def extract(self) -> DataFrame[FHIRObservation] | None:
        df: DataFrame = self.get_query_df(self._source.connection_uri, **self._concept_source.params)

        if df.empty:
            return None

        observation_df = pd.DataFrame()

        observation_df[FHIRObservation.subject] = df.apply(self._apply_subject, axis=1)
        observation_df[FHIRObservation.effective_date_time] = df.apply(self._apply_effective_date_time, axis=1)
        observation_df[FHIRObservation.value_quantity] = df.apply(self._apply_value_quantity, axis=1)
        observation_df[FHIRObservation.code] = df.apply(self._apply_code, axis=1)

        return observation_df.pipe(DataFrame[FHIRObservation])


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

    def extract(self) -> DataFrame[FHIREncounter] | None:
        df: DataFrame = self.get_query_df(self._source.connection_uri, **self._concept_source.params)

        if df.empty:
            return None

        encounter_df = pd.DataFrame()

        encounter_df[FHIREncounter.subject] = df.apply(self._apply_subject, axis=1)
        encounter_df[FHIREncounter.actual_period] = df.apply(self._apply_actual_period, axis=1)
        encounter_df[FHIREncounter.care_team] = df.apply(self._apply_care_team, axis=1)

        return encounter_df.pipe(DataFrame[FHIREncounter])


class DeviceUsageExtractor(PandasDatabaseMixin, ConceptExtractor[FHIRDeviceUsage]):
    def _apply_subject(self, df: DataFrame) -> Reference:
        return Reference(reference=str(df["subject_id"]), type=self._concept_source.source)

    def _apply_timing_date_time(self, df: DataFrame) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["timestamp"], utc=True))

    def _apply_device(self, df: DataFrame) -> CodeableReference:
        return CodeableReference(
            concept=CodeableConcept(
                coding=[
                    Coding(code=str(concept_id), system=concept_type)
                    for concept_type, concept_id in self._concept.identifiers.items()
                ]
            )
        )

    def _apply_status(self, df: DataFrame) -> StatusCodes:
        return StatusCodes.IN_PROGRESS if df["status"] else StatusCodes.ON_HOLD

    def extract(self) -> DataFrame[FHIRDeviceUsage] | None:
        df: DataFrame = self.get_query_df(self._source.connection_uri, **self._concept_source.params)

        if df.empty:
            return None

        device_usage_df = pd.DataFrame()

        device_usage_df[FHIRDeviceUsage.patient] = df.apply(self._apply_subject, axis=1)
        device_usage_df[FHIRDeviceUsage.timing_date_time] = df.apply(self._apply_timing_date_time, axis=1)
        device_usage_df[FHIRDeviceUsage.device] = df.apply(self._apply_device, axis=1)
        device_usage_df[FHIRDeviceUsage.status] = df.apply(self._apply_status, axis=1)

        return device_usage_df.pipe(DataFrame[FHIRDeviceUsage])
