import pandas as pd
from pandera.typing import DataFrame

from open_icu.steps.preprocessing.processor.base import Preprocessor
from open_icu.types.base import SubjectData
from open_icu.types.fhir import (
    CodeableConcept,
    Coding,
    FHIRObjectCondition,
    FHIRObjectDeviceUsage,
    FHIRObjectObservation,
    Reference,
    Stage,
    StatusCodes,
)


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

    def _map_observation(self, observation: DataFrame[FHIRObjectObservation], value_name: str) -> DataFrame:
        df = observation.copy()[
            [
                FHIRObjectObservation.subject,
                FHIRObjectObservation.effective_date_time,
                FHIRObjectObservation.value_quantity,
            ]
        ]

        df = df.rename(
            columns={
                FHIRObjectObservation.subject: "stay_id",
                FHIRObjectObservation.effective_date_time: "charttime",
                FHIRObjectObservation.value_quantity: value_name,
            }
        )

        df[value_name] = df[value_name].map(lambda x: x["value"])
        df["stay_id"] = df["stay_id"].map(lambda x: x["reference"])

        return df.pipe(DataFrame)

    def _map_device_usage(self, device_usage: DataFrame[FHIRObjectDeviceUsage]) -> DataFrame:
        df = device_usage.copy()[
            [
                FHIRObjectDeviceUsage.patient,
                FHIRObjectDeviceUsage.timing_date_time,
                FHIRObjectDeviceUsage.status,
            ]
        ]

        df = df.rename(
            columns={
                FHIRObjectDeviceUsage.patient: "stay_id",
                FHIRObjectDeviceUsage.timing_date_time: "charttime",
                FHIRObjectDeviceUsage.status: "rrt_status",
            }
        )

        df["rrt_status"] = df["rrt_status"].map(lambda x: 1 if StatusCodes.IN_PROGRESS == x else 0)
        df["stay_id"] = df["stay_id"].map(lambda x: x["reference"])

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
                        subject_data.data[self._creatinie].pipe(DataFrame[FHIRObjectObservation]), "creat"
                    ),
                )
            )

        if self._urineoutput and subject_data.data.get(self._urineoutput) is not None:
            dataset.append(
                Dataset(
                    DatasetType.URINEOUTPUT,
                    self._map_observation(
                        subject_data.data[self._urineoutput].pipe(DataFrame[FHIRObjectObservation]), "urineoutput"
                    ),
                )
            )
        if self._demografic and subject_data.data.get(self._demografic) is not None:
            dataset.append(
                Dataset(
                    DatasetType.DEMOGRAPHICS,
                    self._map_observation(
                        subject_data.data[self._demografic].pipe(DataFrame[FHIRObjectObservation]), "weight"
                    ),
                )
            )

        if self._rrt and subject_data.data.get(self._rrt) is not None:
            dataset.append(
                Dataset(
                    DatasetType.RRT,
                    self._map_device_usage(subject_data.data[self._rrt].pipe(DataFrame[FHIRObjectDeviceUsage])),
                )
            )

        if not dataset:
            return subject_data

        aki_data = Analyser(dataset).process_stay(subject_data.id)

        for col in aki_data.filter(like="stage", axis=1):
            assert isinstance(col, str)

            condition_df = pd.DataFrame()
            condition_df[FHIRObjectCondition.code] = aki_data.apply(
                lambda _: CodeableConcept(coding=[Coding(code=f"aki_{col}", system="open_icu")]), axis=1
            )
            condition_df[FHIRObjectCondition.subject] = aki_data.apply(
                lambda _: Reference(reference=subject_data.id, type=subject_data.source), axis=1
            )
            condition_df[FHIRObjectCondition.onset_date_time] = aki_data["charttime"]
            condition_df[FHIRObjectCondition.stage] = aki_data.apply(
                lambda _df: Stage(assessment=[Reference(reference=str(_df[col]), type=col)]), axis=1
            )

            subject_data.data[f"aki_{col}"] = condition_df.dropna().pipe(DataFrame[FHIRObjectCondition]).copy()  # type: ignore[assignment]

        return subject_data
