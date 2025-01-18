import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.preprocessing.processor.base import Preprocessor
from open_icu.types.base import SubjectData
from open_icu.types.fhir import FHIRCondition, FHIRDeviceUsage, FHIRNumericObservation, StatusCodes
from open_icu.types.fhir.utils import to_identifiers_str


class AKIPreprocessor(Preprocessor):
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
        from pyaki.kdigo import Analyser  # type: ignore[import-untyped]
        from pyaki.probes import Dataset, DatasetType  # type: ignore[import-untyped]

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

        for col in aki_data.filter(like="stage", axis=1):
            assert isinstance(col, str)

            condition_df = pd.DataFrame()
            condition_df[FHIRCondition.identifier__coding] = to_identifiers_str({"open_icu": f"aki_{col}"})
            condition_df[FHIRCondition.subject__reference] = subject_data.id
            condition_df[FHIRCondition.subject__type] = subject_data.source
            condition_df[FHIRCondition.onset_date_time] = aki_data["charttime"]
            condition_df[FHIRCondition.stage__assessment] = aki_data[col]

            subject_data.data[f"aki_{col}"] = condition_df.dropna().pipe(DataFrame[FHIRCondition]).copy()  # type: ignore[assignment]

        return subject_data
