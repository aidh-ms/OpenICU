from typing import Any

from pydantic import BaseModel


class UnitConverterConfig(BaseModel):
    """
    A class representing the unit converter configuration.

    Attributes
    ----------
    name : str
        The name of the unit converter.
    description : str
        The description of the unit converter.
    unitconverter : str
        A python dotter path to the unit converter class.
    base_unit : str
        The base unit of the unit converter.
    params : dict[str, Any]
        A dictionary of parameters that will passed to the unit converter class.
    """

    name: str
    description: str
    unitconverter: str
    base_unit: str
    params: dict[str, Any]
