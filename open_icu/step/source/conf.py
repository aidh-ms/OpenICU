from open_icu.conf import ServiceConfiguration
from open_icu.step.source.proto import ISamplerService


class SourceConfig(ServiceConfiguration[ISamplerService]):
    """
    A class representing the source configuration.

    Attributes
    ----------
    name : str
        The name of the source.
    connection_uri : str
        The connection uri of the source.
    args : list[Any], default: []
        The arguments to be passed to the service class.
    kwargs : dict[str, Any], default: {}
        The keyword arguments to be passed to the service class.
    path : str
        Thepython dotted path to the service class.
    service : SourceServiceProto
        a property that returns an instance of the service class.
    """

    name: str
    connection_uri: str
