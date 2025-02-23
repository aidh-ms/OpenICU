from typing import Any

from open_icu.step.unit.conf import UnitConversionConfig
from open_icu.step.unit.proto import IUnitConversionService
from open_icu.type.subject import SubjectData


class SimpleUnitConverter(IUnitConversionService):
    """
    A simple unit converter that uses a dictionary to store the conversion factors.

    Parameters
    ----------
    unit_config : UnitConversionConfig
        The configuration for the unit converter.
    *args : Any
        The arguments to be passed to the service class.
    **kwargs : float
        The conversion factors for the units of measurement
    """

    def __init__(self, unit_config: UnitConversionConfig, *args: Any, **kwargs: float) -> None:
        self._base_unit = unit_config.base_unit
        self._conversion_factor: dict[str, float] = unit_config.kwargs

    def __call__(self, value: float, source_unit: str, target_unit: str, subject_data: SubjectData) -> float:
        """
        Convert the value from the source unit to the target unit.

        Parameters
        ----------
        value : float
            The value to be converted.
        source_unit : str
            The source unit of measurement.
        target_unit : str
            The target unit of measurement.

        Returns
        -------
        float
            The converted value.
        """
        return value * self._conversion_factor[source_unit] / self._conversion_factor[target_unit]

    def supports_conversion(self, source_unit: str, target_unit: str, subject_data: SubjectData) -> bool:
        """
        Check if the converter supports the conversion between the source and target units.

        Parameters
        ----------
        source_unit : str
            The source unit of measurement.
        target_unit : str
            The target unit of measurement.
        subject_data : SubjectData
            The subject data.

        Returns
        -------
        bool
            True if the conversion is supported, False otherwise.
        """
        return source_unit in self._conversion_factor and target_unit in self._conversion_factor
