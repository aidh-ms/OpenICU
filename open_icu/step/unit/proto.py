from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from open_icu.type.subject import SubjectData

if TYPE_CHECKING:
    from open_icu.step.unit.conf import UnitConversionConfig


class IUnitConversionService(Protocol):
    """
    A protocol for the sampler service.
    """

    def __init__(self, unit_config: UnitConversionConfig, *args: Any, **kwargs: Any) -> None:
        ...

    def __call__(self, value: float, source_unit: str, target_unit: str, subject_data: SubjectData) -> float:
        ...

    def supports_conversion(self, source_unit: str, target_unit: str, subject_data: SubjectData) -> bool:
        ...
