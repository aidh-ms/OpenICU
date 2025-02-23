from pydantic import Field

from open_icu.conf import ServiceConfiguration
from open_icu.step.preprocessor.proto import IPreprocessorService


class PreprocessorConfig(ServiceConfiguration[IPreprocessorService]):
    """
    A class representing the source configuration.

    Attributes
    ----------
    name : str
        The name of the chohort filter.
    order : int, default: 0
        The order in which the preprocessor should be applied.
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
    order: int = Field(default=0)
