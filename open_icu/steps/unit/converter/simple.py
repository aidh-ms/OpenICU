from open_icu.steps.unit.converter.base import UnitConverter


class SimpleUnitConverter(UnitConverter):
    def __init__(self, base_unit: str, **kwargs: float) -> None:
        super().__init__(base_unit=base_unit)

        self._conversion_factor = kwargs

    def convert(self, value: float, source_unit: str, target_unit: str) -> float:
        return value * self._conversion_factor[source_unit] / self._conversion_factor[target_unit]

    def get_units(self) -> list[str]:
        return list(self._conversion_factor.keys())
