from typing import Literal

import pandas as pd
from pydantic import BaseModel

from open_icu.step.cohort.conf import CohortConfig
from open_icu.step.cohort.proto import ICohortService
from open_icu.type.subject import SubjectData


class Concept(BaseModel):
    """
    A class representing a concept.

    Attributes
    ----------
    name : str
        The name of the concept.
    start : str
        The start date of the concept.
    end : str
        The end date of the concept.
    """

    name: str
    start: str
    end: str


class TimeframeFilter(ICohortService):
    """
    A filter that filters subjects based on the timeframe of their data.

    Parameters
    ----------
    cohort_config : CohortConfig
        The cohort configuration.
    concepts : list[dict[str, str]], default []
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
        cohort_config: CohortConfig,
        concepts: list[dict[str, str]] | None = None,
        days: int = 0,
        seconds: int = 0,
        microseconds: int = 0,
        milliseconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
        weeks: int = 0,
        strategy: Literal["any"] | Literal["all"] = "any",
    ) -> None:
        self._cohort_config = cohort_config
        self._days = days
        self._seconds = seconds
        self._microseconds = microseconds
        self._milliseconds = milliseconds
        self._minutes = minutes
        self._hours = hours
        self._weeks = weeks
        self._strategy = strategy

        if isinstance(concepts, list):
            self._concepts = [Concept(**concept) for concept in concepts]
        else:
            self._concepts = concepts or []

    def __call__(self, subject_data: SubjectData) -> bool:
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
        for concept in self._concepts:
            if (concept_data := subject_data.data.get(concept.name, None)) is None:
                return True

            if concept.start not in concept_data.columns or concept.end not in concept_data.columns:
                return True

            td = pd.Timedelta(
                days=self._days,
                seconds=self._seconds,
                microseconds=self._microseconds,
                milliseconds=self._milliseconds,
                minutes=self._minutes,
                hours=self._hours,
                weeks=self._weeks,
            )

            period = concept_data[concept.end] - concept_data[concept.start]

            if self._strategy == "all":
                return not (period >= td).all()

            return not (period >= td).any()

        return False
