from typing import Literal

import pandas as pd
from pandera.errors import SchemaError
from pandera.typing import DataFrame

from open_icu.steps.cohort.filters.base import CohortFilter
from open_icu.types.base import SubjectData
from open_icu.types.fhir.encounter import FHIREncounter


class TimeframeFilter(CohortFilter):
    """
    A filter that filters subjects based on the timeframe of their data.

    Parameters
    ----------
    concepts : list[str]
        The concepts to filter on.
    days : int, default 0
        The number of days.
    seconds : int, default 0
        The number of seconds.
    microseconds : int, default 0
        The number of microseconds.
    milliseconds : int, default 0
        The number of milliseconds.
    minutes : int, default 0
        The number of minutes.
    hours : int, default 0
        The number of hours.
    weeks : int, default 0
        The number of weeks.
    strategy : {"any", "all"}, default "any"
        The strategy to use when filtering the data. If "any", the filter will
        return True if any of the timeframes fit the criteria.
    """

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

    def _get_delta(self, df: DataFrame[FHIREncounter]) -> pd.Timedelta:
        """
        calculate the time delta between the start and end of the encounter

        Parameters
        ----------
        df : DataFrame[FHIREncounter]
            The DataFrame containing the encounter data.

        Returns
        -------
        pd.Timedelta
            The time delta between the start and end of the encounter.
        """
        dt = df[FHIREncounter.actual_period__end] - df[FHIREncounter.actual_period__start]
        assert isinstance(dt, pd.Timedelta)
        return dt

    def filter(self, subject_data: SubjectData) -> bool:
        """
        Filter out subjects based on the timeframe of their data.
        if the strategy is "any", the filter will return False if any of the timeframes is larger as the criteria.
        if the strategy is "all", the filter will return False if all of the timeframes is larger as the criteria.
        if all timeframes are smaller as the criteria, the filter will return True and the subject will be excluded.
        if no data is available for the concepts, the filter will return True and the subject will be excluded.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to filter.

        Returns
        -------
        bool
            Whether the subject should be included or not.
        """
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
                return not (concept_data.apply(self._get_delta, axis=1) >= td).all()

            return not (concept_data.apply(self._get_delta, axis=1) >= td).any()

        return True
