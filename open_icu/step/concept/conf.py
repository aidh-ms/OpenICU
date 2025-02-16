from typing import Any

from open_icu.conf import Configuration, ServiceConfiguration


class ConceptSourceConfig(ServiceConfiguration):
    """
    A class representing the source configuration.

    Attributes
    ----------
    source : str
        The source of the concept.
    unit : dict[str, str]
        A dictionary mapping between values and units used in the source.
    args : list[Any], default: []
        The arguments to be passed to the service class.
    kwargs : dict[str, Any], default: {}
        The keyword arguments to be passed to the service class.
    path : str
        Thepython dotted path to the service class.
    service : SourceServiceProto
        a property that returns an instance of the service class.
    """

    source: str
    unit: dict[str, str]


class ConceptConfig(Configuration):
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
    limits: dict[str, str]
    sources: list[ConceptSourceConfig]

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
