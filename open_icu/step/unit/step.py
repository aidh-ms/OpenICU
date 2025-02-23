from pathlib import Path

from open_icu.conf import load_yaml_configs
from open_icu.step.base import BaseStep
from open_icu.step.concept.conf import ConceptConfig
from open_icu.step.proto import StepProto
from open_icu.step.unit.conf import UnitConversionConfig
from open_icu.type.subject import SubjectData


class UnitConversionStep(BaseStep):
    """
    A step that extracts subjects from a source.

    Parameters
    ----------
    source_configs : Path | list[SourceConfig]
        The source configurations.
    fail_silently : bool, default: False
        Whether to fail silently or not.
    parent : StepProto, default: None
        The parent step.
    """

    def __init__(
        self,
        unit_configs: Path | list[UnitConversionConfig],
        concept_configs: Path | list[ConceptConfig],
        fail_silently: bool = False,
        parent: StepProto | None = None,
    ) -> None:
        super().__init__(fail_silently, parent)

        self._unit_configs: list[UnitConversionConfig] = []
        if isinstance(unit_configs, list):
            self._unit_configs = unit_configs
        elif isinstance(unit_configs, Path):
            self._unit_configs = load_yaml_configs(unit_configs, UnitConversionConfig)

        self._concept_configs: list[ConceptConfig] = []
        if isinstance(concept_configs, list):
            self._concept_configs = concept_configs
        elif isinstance(concept_configs, Path):
            self._concept_configs = load_yaml_configs(concept_configs, ConceptConfig)

    def supports_conversion(self, source_unit: str, target_unit: str, subject_data: SubjectData) -> bool:
        """
        Check if the conversion between the source and target units is supported
        by any of the converters.

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
        for converter in self._unit_configs:
            if converter.service.supports_conversion(source_unit, target_unit, subject_data):
                return True

        return False

    def convert(self, value: float, source_unit: str, target_unit: str, subject_data: SubjectData) -> float:
        """
        Convert the value from the source unit to the target unit.

        Parameters
        ----------
        value : float
            The value to be converted
        source_unit : str
            The source unit of measurement
        target_unit : str
            The target unit of measurement
        """
        if source_unit == target_unit:
            return value

        for converter in self._unit_configs:
            if converter.service.supports_conversion(source_unit, target_unit, subject_data):
                return converter.service(value, source_unit, target_unit, subject_data)

        return value

    def process(self, subject_data: SubjectData) -> SubjectData:
        for concept in self._concept_configs:
            if (df := subject_data.data.get(concept.name)) is None:
                continue

            if not (field_units := concept.unit):
                continue

            for field, unit in field_units.items():
                # check if the field and unit columns are present
                if not {f"{field}__value", f"{field}__unit"}.issubset(df.columns):
                    continue

                # validate if all unit conversions are supported
                if not all(
                    self.supports_conversion(source_unit, unit, subject_data)
                    for source_unit in df[f"{field}__unit"].unique()
                ):
                    continue

                # validate if all units are the same
                if all(source_unit == unit for source_unit in df[f"{field}__unit"].unique()):
                    continue

                df[f"{field}__value"] = df.apply(
                    lambda x: self.convert(x[f"{field}__value"], x[f"{field}__unit"], unit, subject_data), axis=1
                )
                df[f"{field}__unit"] = unit

            subject_data.data[concept.name] = df

        return subject_data
