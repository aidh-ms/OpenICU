from typing import Annotated, Any, cast

import pandas as pd
from pandera.typing import DataFrame

from open_icu.step.concept.conf import ConceptConfig
from open_icu.step.concept.services.concept.base import BaseConceptExtractor
from open_icu.type.fhir import FHIRDeviceUsage
from open_icu.type.fhir.types import StatusCodes


class DeviceUsageExtractor(BaseConceptExtractor[FHIRDeviceUsage]):
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

    def _apply_timing_date_time(
        self, df: DataFrame, concept_config: ConceptConfig
    ) -> Annotated[pd.DatetimeTZDtype, "ns", "utc"]:
        """
        A method to apply the timing date time to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the timing date time to.
        concept_config : ConceptConfig
            The concept configuration.

        Returns
        -------
        Annotated[pd.DatetimeTZDtype, "ns", "utc"]
            The timing date time.
        """
        return cast(Annotated[pd.DatetimeTZDtype, "ns", "utc"], pd.to_datetime(df["timestamp"], utc=True))

    def _apply_status(self, df: DataFrame, concept_config: ConceptConfig) -> StatusCodes:
        """
        A method to apply the status to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the status to.
        concept_config : ConceptConfig
            The concept configuration.

        Returns
        -------
        StatusCodes
            The device status.
        """
        return StatusCodes.IN_PROGRESS if df["status"] else StatusCodes.ON_HOLD

    def __call__(
        self, concept_config: ConceptConfig, subject_id: str, *args: Any, **kwargs: Any
    ) -> DataFrame[FHIRDeviceUsage] | None:
        """
        A method to extract device usage data from the database.

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
        DataFrame[FHIRDeviceUsage] | None
            The extracted device usage data.
        """
        df: DataFrame = self._get_data(subject_id)

        if df.empty:
            return None

        device_usage_df = pd.DataFrame()

        device_usage_df[FHIRDeviceUsage.identifier__coding] = df.apply(
            self._apply_identifier__coding, args=(concept_config,), axis=1
        )
        device_usage_df[FHIRDeviceUsage.subject__reference] = df.apply(
            self._apply_subject__reference, args=(concept_config,), axis=1
        )
        device_usage_df[FHIRDeviceUsage.subject__type] = df.apply(
            self._apply_subject__type, args=(concept_config,), axis=1
        )

        device_usage_df[FHIRDeviceUsage.timing_date_time] = df.apply(
            self._apply_timing_date_time, args=(concept_config,), axis=1
        )
        device_usage_df[FHIRDeviceUsage.status] = df.apply(self._apply_status, args=(concept_config,), axis=1)

        return device_usage_df.pipe(DataFrame[FHIRDeviceUsage])
