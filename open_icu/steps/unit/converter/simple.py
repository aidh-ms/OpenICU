from open_icu.steps.unit.converter.base import UnitConverter


class SimpleUnitConverter(UnitConverter):
    """
    A simple unit converter that uses a dictionary to store the conversion factors.

    Parameters
    ----------
    base_unit : str
        The base unit of measurement
    **kwargs : float
        The conversion factors for the units of measurement
    """

    def __init__(self, base_unit: str, **kwargs: float) -> None:
        super().__init__(base_unit=base_unit)

        self._conversion_factor = kwargs

    def convert(self, value: float, source_unit: str, target_unit: str) -> float:
        """
        Convert the value from the source unit to the target unit.
        """
        return value * self._conversion_factor[source_unit] / self._conversion_factor[target_unit]

    def get_units(self) -> list[str]:
        """
        Get the units of measurement supported by the converter.
        """
        return list(self._conversion_factor.keys())
