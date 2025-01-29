from pydantic import BaseModel


class SampleConfig(BaseModel):
    """
    A class representing the sample configuration.

    Attributes
    ----------
    samples : list[str]
        A list of samples.
    sampler : str
        A python dotter path to the sampler class.
    params : dict[str, str]
        A dictionary of parameters that will passed to the sampler class.
    """

    samples: list[str] = []
    sampler: str
    params: dict[str, str]


class SourceConfig(BaseModel):
    """
    A class representing the source configuration.

    Attributes
    ----------
    name : str
        The name of the source.
    connection_uri : str
        The connection uri of the source.
    sample : SampleConfig
        The sample configuration.
    """

    name: str
    connection_uri: str
    sample: SampleConfig
