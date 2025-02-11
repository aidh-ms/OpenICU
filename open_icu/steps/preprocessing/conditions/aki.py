import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.preprocessing.processor.base import Preprocessor
from open_icu.types.base import SubjectData
from open_icu.types.fhir import FHIRCondition, FHIRDeviceUsage, FHIRNumericObservation, StatusCodes
from open_icu.types.fhir.utils import to_identifiers_str


class AKIPreprocessor(Preprocessor):
    """
    A preprocessor that processes AKI data and add aki stages for a subject.

    Parameters
    ----------
    concepts : list[str]
        The concepts to use for aki classification.
    demografic : str
        The concept for demographics.
    creatinie : str
        The concept for creatinine.
    rrt : str
        The concept for rrt.
    urineoutput : str
        The concept for urine output
    """

    def __init__(
        self,
        concepts: list[str],
        demografic: str = "",
        creatinie: str = "",
        rrt: str = "",
        urineoutput: str = "",
    ) -> None:
        super().__init__(concepts)

        self._demografic = demografic
        self._creatinie = creatinie
        self._rrt = rrt
        self._urineoutput = urineoutput

    def _map_observation(self, observation: DataFrame[FHIRNumericObservation], value_name: str) -> DataFrame:
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
                FHIRNumericObservation.value_quantity__value: value_name,
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

        df["rrt_status"] = df["rrt_status"].map(lambda x: 1 if StatusCodes.IN_PROGRESS == x else 0)

        return df.pipe(DataFrame)

    def process(self, subject_data: SubjectData) -> SubjectData:
        """
        A method to process the subject data with the AKI preprocessor.
        This method will add the AKI stages to the subject data.
        This is done by using the pyAKI library.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to preprocess.

        Returns
        -------
        SubjectData
            The preprocessed subject data with added aki stages.
        """
        from pyaki.kdigo import Analyser
        from pyaki.probes import Dataset, DatasetType

        dataset: list[Dataset] = []

        if self._creatinie and subject_data.data.get(self._creatinie) is not None:
            dataset.append(
                Dataset(
                    DatasetType.CREATININE,
                    self._map_observation(
                        subject_data.data[self._creatinie].pipe(DataFrame[FHIRNumericObservation]), "creat"
                    ),
                )
            )

        if self._urineoutput and subject_data.data.get(self._urineoutput) is not None:
            dataset.append(
                Dataset(
                    DatasetType.URINEOUTPUT,
                    self._map_observation(
                        subject_data.data[self._urineoutput].pipe(DataFrame[FHIRNumericObservation]), "urineoutput"
                    ),
                )
            )
        if self._demografic and subject_data.data.get(self._demografic) is not None:
            dataset.append(
                Dataset(
                    DatasetType.DEMOGRAPHICS,
                    self._map_observation(
                        subject_data.data[self._demografic].pipe(DataFrame[FHIRNumericObservation]), "weight"
                    ),
                )
            )

        if self._rrt and subject_data.data.get(self._rrt) is not None:
            dataset.append(
                Dataset(
                    DatasetType.RRT,
                    self._map_device_usage(subject_data.data[self._rrt].pipe(DataFrame[FHIRDeviceUsage])),
                )
            )

        if not dataset:
            return subject_data

        aki_data = Analyser(dataset).process_stay(subject_data.id)
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
