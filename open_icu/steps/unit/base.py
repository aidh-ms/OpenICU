from importlib import import_module
from pathlib import Path

from open_icu.steps.base import BaseStep
from open_icu.steps.unit.converter.base import UnitConverter
from open_icu.types.base import SubjectData
from open_icu.types.conf.concept import ConceptConfig
from open_icu.types.conf.unit import UnitConverterConfig


class UnitConversionStep(BaseStep[UnitConverterConfig]):
    def __init__(
        self,
        configs: Path | list[UnitConverterConfig] | None = None,
        concept_configs: Path | list[ConceptConfig] | None = None,
        parent: BaseStep | None = None,
    ) -> None:
        super().__init__(configs=configs, concept_configs=concept_configs, parent=parent)

        self._unit_converter_conigs: list[UnitConverterConfig] = []
        if isinstance(configs, list):
            self._unit_converter_conigs = configs
        elif self._config_path is not None:
            self._unit_converter_conigs = self._read_config(self._config_path / "units", UnitConverterConfig)

        self._converter: list[UnitConverter] = []
        for conf in self._unit_converter_conigs:
            module_name, cls_name = conf.unitconverter.rsplit(".", 1)
            module = import_module(module_name)
            cls = getattr(module, cls_name)
            converter: UnitConverter = cls(base_unit=conf.base_unit, **conf.params)

            self._converter.append(converter)

    def supports_conversion(self, source_unit: str, target_unit: str) -> bool:
        for converter in self._converter:
            if converter.supports_conversion(source_unit, target_unit):
                return True

        return False

    def convert(self, value: float, source_unit: str, target_unit: str) -> float:
        for converter in self._converter:
            if converter.supports_conversion(source_unit, target_unit):
                return converter.convert(value, source_unit, target_unit)

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
                    self.supports_conversion(source_unit, unit) for source_unit in df[f"{field}__unit"].unique()
                ):
                    continue

                # validate if all units are the same
                if all(source_unit == unit for source_unit in df[f"{field}__unit"].unique()):
                    continue

                df[f"{field}__value"] = df.apply(
                    lambda x: self.convert(x[f"{field}__value"], x[f"{field}__unit"], unit), axis=1
                )
                df[f"{field}__unit"] = unit

            subject_data.data[concept.name] = df

        return subject_data
