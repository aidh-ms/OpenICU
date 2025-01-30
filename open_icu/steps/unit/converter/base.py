from abc import ABC, abstractmethod


class UnitConverter(ABC):
    """
    A base class for implementing unit converters.

    Parameters
    ----------
    base_unit : str
        The base unit of measurement
    """

    def __init__(self, base_unit: str = "") -> None:
        super().__init__()

        self._base_unit = base_unit

    def __call__(self, value: float, source_unit: str, target_unit: str) -> float:
        """
        Shortcut for the convert method.

        Parameters
        ----------
        value : float
            The value to be converted
        source_unit : str
            The source unit of measurement
        target_unit : str
            The target unit of measurement

        Returns
        -------
        float
            The converted value
        """
        return self.convert(value, source_unit, target_unit)

    @abstractmethod
    def convert(self, value: float, source_unit: str, target_unit: str) -> float:
        """
        Abstract method for converting the value from the source unit to the target unit.

        Parameters
        ----------
        value : float
            The value to be converted
        source_unit : str
            The source unit of measurement
        target_unit : str
            The target unit of measurement
        """
        raise NotImplementedError

    @abstractmethod
    def get_units(self) -> list[str]:
        """
        Abstract method for getting the units of measurement supported by the converter.
        """
        raise NotImplementedError

    def supports_conversion(self, source_unit: str, target_unit: str) -> bool:
        """
        Check if the conversion between the source and target units is supported.

        Parameters
        ----------
        source_unit : str
            The source unit of measurement
        target_unit : str
            The target unit of measurement

        Returns
        -------
        bool
            True if the conversion is supported, False otherwise.
        """
        units = self.get_units()
        return source_unit in units and target_unit in units
