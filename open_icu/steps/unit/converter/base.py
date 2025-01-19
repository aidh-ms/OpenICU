from abc import ABC, abstractmethod


class UnitConverter(ABC):
    def __init__(self, base_unit: str = "") -> None:
        super().__init__()

        self._base_unit = base_unit

    def __call__(self, value: float, source_unit: str, target_unit: str) -> float:
        return self.convert(value, source_unit, target_unit)

    @abstractmethod
    def convert(self, value: float, source_unit: str, target_unit: str) -> float:
        raise NotImplementedError

    @abstractmethod
    def get_units(self) -> list[str]:
        raise NotImplementedError

    def supports_conversion(self, source_unit: str, target_unit: str) -> bool:
        units = self.get_units()
        return source_unit in units and target_unit in units
