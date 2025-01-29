from typing import Any

from pydantic import BaseModel


class CohortFilterConfig(BaseModel):
    """
    A class representing the cohort filter step configuration.

    Attributes
    ----------
    name : str
        The name of the cohort filter step.
    description : str
        The description of the cohort filter step.
    concepts : list[str]
        A list of concepts used in the filter.
    filter : str
        A python dotter path to the filter class.
    params : dict[str, Any]
        A dictionary of parameters that will passed to the filter class.
    """

    name: str
    description: str
    concepts: list[str]
    filter: str
    params: dict[str, Any]
