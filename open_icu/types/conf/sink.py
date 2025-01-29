from typing import Any

from pydantic import BaseModel


class SinkConfig(BaseModel):
    """
    A class representing the sink configuration.

    Attributes
    ----------
    name : str
        The name of the sink.
    description : str
        The description of the sink.
    sink : str
        A python dotter path to the sink class.
    params : dict[str, Any]
        A dictionary of parameters that will passed to the sink class
    """

    name: str
    description: str
    sink: str
    params: dict[str, Any]
