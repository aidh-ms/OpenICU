from typing import Annotated, cast

import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.source.concept.base import ConceptExtractor
from open_icu.steps.source.database import PandasDatabaseMixin
from open_icu.types.fhir import FHIRDeviceUsage, StatusCodes


class DeviceUsageExtractor(PandasDatabaseMixin, ConceptExtractor[FHIRDeviceUsage]):
    """
    A class to extract device usage data from a database.

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

    def _apply_timing_date_time(self, df: DataFrame) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        """
        A method to apply the timing date time to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the timing date time to.

        Returns
        -------
        Annotated[pd.DatetimeTZDtype, "ns", "utc"]
            The timing date time.
        """
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["timestamp"], utc=True))

    def _apply_status(self, df: DataFrame) -> StatusCodes:
        """
        A method to apply the status to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the status to.

        Returns
        -------
        StatusCodes
            The device status.
        """
        return StatusCodes.IN_PROGRESS if df["status"] else StatusCodes.ON_HOLD

    def extract(self) -> DataFrame[FHIRDeviceUsage] | None:
        """
        A method to extract device usage data from the database.

        Returns
        -------
        DataFrame[FHIRDeviceUsage] | None
            The extracted device usage data.
        """
        df: DataFrame = self.get_query_df(self._source.connection_uri, **self._concept_source.params)

        if df.empty:
            return None

        device_usage_df = pd.DataFrame()

        device_usage_df[FHIRDeviceUsage.identifier__coding] = df.apply(self._apply_identifier__coding, axis=1)
        device_usage_df[FHIRDeviceUsage.subject__reference] = df.apply(self._apply_subject__reference, axis=1)
        device_usage_df[FHIRDeviceUsage.subject__type] = df.apply(self._apply_subject__type, axis=1)

        device_usage_df[FHIRDeviceUsage.timing_date_time] = df.apply(self._apply_timing_date_time, axis=1)
        device_usage_df[FHIRDeviceUsage.status] = df.apply(self._apply_status, axis=1)

        return device_usage_df.pipe(DataFrame[FHIRDeviceUsage])
