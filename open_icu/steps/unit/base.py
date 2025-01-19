from importlib import import_module
from pathlib import Path

from open_icu.steps.base import BaseStep
from open_icu.steps.unit.converter.base import UnitConverter
from open_icu.types.base import SubjectData
from open_icu.types.conf.unit import UnitConverterConf


class UnitConversionStep(BaseStep):
    def __init__(
        self, config_path: Path | None = None, concept_path: Path | None = None, parent: BaseStep | None = None
    ) -> None:
        super().__init__(config_path=config_path, concept_path=concept_path, parent=parent)

        self._unit_converter_conigs: list[UnitConverterConf] = []
        self._converter: list[UnitConverter] = []
        if config_path is not None:
            self._unit_converter_conigs = self._read_config(config_path / "units", UnitConverterConf)

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
        for concept in self._concepts:
            if (df := subject_data.data.get(concept.name)) is None:
                continue

            if not (field_units := concept.unit):
                continue

            for field, unit in field_units.items():
                print("1", concept.name, field, unit)

                # check if the field and unit columns are present
                if not {f"{field}__value", f"{field}__unit"}.issubset(df.columns):
                    continue

                print("2")

                # validate if all unit conversions are supported
                if not all(
                    self.supports_conversion(source_unit, unit) for source_unit in df[f"{field}__unit"].unique()
                ):
                    continue

                print("3")

                # validate if all units are the same
                if all(source_unit == unit for source_unit in df[f"{field}__unit"].unique()):
                    continue

                print("4")

                df[f"{field}__value"] = df.apply(
                    lambda x: self.convert(x[f"{field}__value"], x[f"{field}__unit"], unit), axis=1
                )
                df[f"{field}__unit"] = unit

            subject_data.data[concept.name] = df

        return subject_data
