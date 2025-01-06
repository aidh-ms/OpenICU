from typing import Literal

import pandas as pd
from pandera.errors import SchemaError

from open_icu.steps.cohort.filters.base import CohortFilter
from open_icu.types.base import SubjectData
from open_icu.types.fhir import FHIREncounter, Period


class TimeframeFilter(CohortFilter):
    def __init__(
        self,
        concepts: list[str],
        days: int = 0,
        seconds: int = 0,
        microseconds: int = 0,
        milliseconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
        weeks: int = 0,
        strategy: Literal["any"] | Literal["all"] = "any",
    ) -> None:
        super().__init__(concepts)

        self._days = days
        self._seconds = seconds
        self._microseconds = microseconds
        self._milliseconds = milliseconds
        self._minutes = minutes
        self._hours = hours
        self._weeks = weeks
        self._strategy = strategy

    def _get_delta(self, period: Period) -> pd.Timedelta:
        return period["end"] - period["start"]

    def filter(self, subject_data: SubjectData) -> bool:
        for concept in self.concepts:
            if (concept_data := subject_data.data.get(concept, None)) is None:
                continue

            try:
                FHIREncounter.validate(concept_data)
            except SchemaError:
                continue

            td = pd.Timedelta(
                days=self._days,
                seconds=self._seconds,
                microseconds=self._microseconds,
                milliseconds=self._milliseconds,
                minutes=self._minutes,
                hours=self._hours,
                weeks=self._weeks,
            )

            if self._strategy == "all":
                return not (concept_data[FHIREncounter.actual_period].map(self._get_delta) >= td).all()

            return not (concept_data[FHIREncounter.actual_period].map(self._get_delta) >= td).any()

        return False
