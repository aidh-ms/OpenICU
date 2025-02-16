from open_icu.conf import ServiceConfiguration
from open_icu.step.unit.proto import IUnitConversionService


class UnitConversionConfig(ServiceConfiguration[IUnitConversionService]):
    """
    A class representing the source configuration.

    Attributes
    ----------
    base_unit : str
        The base unit of the conversion.
    args : list[Any], default: []
        The arguments to be passed to the service class.
    kwargs : dict[str, Any], default: {}
        The keyword arguments to be passed to the service class.
    path : str
        Thepython dotted path to the service class.
    service : SourceServiceProto
        a property that returns an instance of the service class.
    """

    base_unit: str
