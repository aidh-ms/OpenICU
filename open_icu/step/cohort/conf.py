from open_icu.conf import ServiceConfiguration
from open_icu.step.cohort.proto import ICohortService


class CohortConfig(ServiceConfiguration[ICohortService]):
    """
    A class representing the source configuration.

    Attributes
    ----------
    name : str
        The name of the chohort filter.
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
