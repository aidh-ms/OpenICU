from typing import Any

import pandas as pd
from pandera.typing import DataFrame

from open_icu.step.preprocessor.conf import PreprocessorConfig
from open_icu.step.preprocessor.proto import IPreprocessorService
from open_icu.type.fhir.condition import FHIRCondition
from open_icu.type.fhir.deviceusage import FHIRDeviceUsage
from open_icu.type.fhir.observation import FHIRNumericObservation
from open_icu.type.fhir.utils import to_identifiers_str
from open_icu.type.subject import SubjectData


class AKIPreprocessor(IPreprocessorService):
    """
    A preprocessor that processes AKI data and add aki stages for a subject.

    Parameters
    ----------
    preprocessor_config : PreprocessorConfig
        The preprocessor configuration.
    demografic : str
        The concept for demographics.
    creatinie : str
        The concept for creatinine.
    rrt : str
        The concept for rrt.
    urineoutput : str
        The concept for urine output
    args : Any
        Additional arguments.
    kwargs : Any
        Additional keyword arguments.
    """

    def __init__(
        self,
        preprocessor_config: PreprocessorConfig,
        demografic: str = "",
        creatinie: str = "",
        rrt: str = "",
        urineoutput: str = "",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._preprocessor_config = preprocessor_config
        self._demografic = demografic
        self._creatinie = creatinie
        self._rrt = rrt
        self._urineoutput = urineoutput

    def _map_observation(self, observation: DataFrame[FHIRNumericObservation], column_name: str) -> DataFrame:
        """
        A method to map the observation data to a DataFrame that is pyAKI compatible.

        Parameters
        ----------
        observation : DataFrame[FHIRNumericObservation]
            The observation data to map.
        value_name : str
            The name of the value to map.

        Returns
        -------
        DataFrame
            The pyAKI compatible DataFrame.
        """
        df = observation.copy()[
            [
                FHIRNumericObservation.subject__reference,
                FHIRNumericObservation.effective_date_time,
                FHIRNumericObservation.value_quantity__value,
            ]
        ]

        df = df.rename(
            columns={
                FHIRNumericObservation.subject__reference: "stay_id",
                FHIRNumericObservation.effective_date_time: "charttime",
                FHIRNumericObservation.value_quantity__value: column_name,
            }
        )

        return df.pipe(DataFrame)

    def _map_device_usage(self, device_usage: DataFrame[FHIRDeviceUsage]) -> DataFrame:
        """
        A method to map the device usage data to a DataFrame that is pyAKI compatible.

        Parameters
        ----------
        device_usage : DataFrame[FHIRDeviceUsage]
            The device usage data to map.

        Returns
        -------
        DataFrame
            The pyAKI compatible DataFrame.
        """
        df = device_usage.copy()[
            [
                FHIRDeviceUsage.subject__reference,
                FHIRDeviceUsage.timing_date_time,
                FHIRDeviceUsage.status,
            ]
        ]

        df = df.rename(
            columns={
                FHIRDeviceUsage.subject__reference: "stay_id",
                FHIRDeviceUsage.timing_date_time: "charttime",
                FHIRDeviceUsage.status: "rrt_status",
            }
        )

        return df.pipe(DataFrame)

    def __call__(self, subject_data: SubjectData, *args: Any, **kwargs: Any) -> SubjectData:
        """
        A method to process the subject data with the AKI preprocessor.
        This method will add the AKI stages to the subject data.
        This is done by using the pyAKI library.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to preprocess.
        args : Any
            Additional arguments.
        kwargs : Any
            Additional keyword arguments

        Returns
        -------
        SubjectData
            The preprocessed subject data with added aki stages.
        """
        try:
            from pyaki.kdigo import Analyser
            from pyaki.utils import Dataset, DatasetType
        except ImportError:
            return subject_data

        datasets: list[Dataset] = []
        observation_mapping = {
            self._creatinie: (DatasetType.CREATININE, "creat"),
            self._urineoutput: (DatasetType.URINEOUTPUT, "urineoutput"),
            self._demografic: (DatasetType.DEMOGRAPHICS, "weight"),
        }

        for dataset_name, (dataset_type, column_name) in observation_mapping.items():
            if not dataset_name or subject_data.data.get(dataset_name, None) is None:
                continue

            datasets.append(
                Dataset(
                    dataset_type,
                    self._map_observation(
                        subject_data.data[dataset_name].pipe(DataFrame[FHIRNumericObservation]), column_name
                    ),
                )
            )

        if self._rrt and subject_data.data.get(self._rrt) is not None:
            datasets.append(
                Dataset(
                    DatasetType.RRT,
                    self._map_device_usage(subject_data.data[self._rrt].pipe(DataFrame[FHIRDeviceUsage])),
                )
            )

        if not datasets:
            return subject_data

        aki_data = Analyser(datasets).process_stay(subject_data.id)
        aki_data = aki_data.drop(columns=["charttime", "stay_id"])
        aki_data = aki_data.reset_index()

        for col in aki_data.filter(like="stage", axis=1):
            assert isinstance(col, str)

            condition_df = pd.DataFrame()
            condition_df[FHIRCondition.onset_date_time] = aki_data["charttime"].dt.tz_localize("UTC")
            condition_df[FHIRCondition.identifier__coding] = to_identifiers_str({"open_icu": f"aki_{col}"})
            condition_df[FHIRCondition.subject__reference] = subject_data.id
            condition_df[FHIRCondition.subject__type] = subject_data.source
            condition_df[FHIRCondition.stage__assessment] = aki_data[col]

            condition_df = condition_df.dropna(subset=[FHIRCondition.stage__assessment])
            condition_df[FHIRCondition.stage__assessment] = (
                condition_df[FHIRCondition.stage__assessment].astype(int).astype(str)
            )

            subject_data.data[f"aki_{col}"] = condition_df.reset_index(drop=True).pipe(DataFrame[FHIRCondition]).copy()  # type: ignore[assignment]

        return subject_data
