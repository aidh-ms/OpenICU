from typing import Any

from pydantic import BaseModel


class ConceptLimits(BaseModel):
    """
    A class representing the limits of a concept.

    Attributes
    ----------
    upper : str
        The upper limit of the concept.
    lower : str
        The lower limit of the concept
    """

    upper: str
    lower: str


class ConceptSource(BaseModel):
    """
    A class representing the source of a concept.

    Attributes
    ----------
    source : str
        The source of the concept.
    extractor : str
        A python dotter path to the extractor class.
    unit : dict[str, str]
        A dictionary mapping between values and units used in the source.
    params : dict[str, Any]
        A dictionary of parameters that will passed to the extractor class.
    """

    source: str
    extractor: str
    unit: dict[str, str]
    params: dict[str, Any]


class ConceptConfig(BaseModel):
    """
    A class representing the concept configuration.

    Attributes
    ----------
    name : str
        The name of the concept.
    description : str
        The description of the concept.
    identifiers : dict[str, str]
        A dictionary of identifiers used in the concept.
    unit : dict[str, str]
        A dictionary mapping between values and units used in the concept.
    limits : ConceptLimits
        The limits of the concept.
    sources : list[ConceptSource]
        A list of sources used in the concept.
    """

    name: str
    description: str
    identifiers: dict[str, str]
    unit: dict[str, str]
    limits: ConceptLimits
    sources: list[ConceptSource]

    def model_post_init(self, __context: Any) -> None:
        """
        The post init method is used to set the open_icu identifier to the name of the concept.

        Parameters
        ----------
        __context : Any
            The context of the model.
        """

        super().model_post_init(__context)

        self.identifiers["open_icu"] = self.name
