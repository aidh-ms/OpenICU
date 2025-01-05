from typing import Annotated, cast

import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.source.concept.base import ConceptExtractor
from open_icu.steps.source.database import PandasDatabaseMixin
from open_icu.types.fhir import (
    CodeableReference,
    FHIRDeviceUsage,
    Reference,
    StatusCodes,
)


class DeviceUsageExtractor(PandasDatabaseMixin, ConceptExtractor[FHIRDeviceUsage]):
    def _apply_subject(self, df: DataFrame) -> Reference:
        return Reference(reference=str(df["subject_id"]), type=self._concept_source.source)

    def _apply_timing_date_time(self, df: DataFrame) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["timestamp"], utc=True))

    def _apply_device(self, df: DataFrame) -> CodeableReference:
        return CodeableReference(concept=self._get_concept_identifiers())

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
