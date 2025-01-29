from typing import Any

from pydantic import BaseModel


class PreprocessorConfig(BaseModel):
    """
    A class representing the preprocessor step configuration.

    Attributes
    ----------
    name : str
        The name of the preprocessor step.
    description : str
        The description of the preprocessor step.
    concepts : list[str]
        A list of concepts used in the preprocessor.
    preprocessor : str
        A python dotter path to the preprocessor class.
    params : dict[str, Any]
        A dictionary of parameters that will passed to the preprocessor class.
    """

    name: str
    description: str
    concepts: list[str]
    preprocessor: str
    params: dict[str, Any]
