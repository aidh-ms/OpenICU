from importlib import import_module
from pathlib import Path

from open_icu.steps.base import BaseStep
from open_icu.steps.preprocessing.preprocessing.base import Preprocessor
from open_icu.types.base import SubjectData
from open_icu.types.conf.preprocessing import PreprocessorConf


class SubjectPreprocessingStep(BaseStep):
    def __init__(self, config_path: Path | None = None, parent: BaseStep | None = None) -> None:
        super().__init__(config_path, parent)

        self._filter_conigs: list[PreprocessorConf] = []
        if config_path is not None:
            self._filter_conigs = self._read_config(config_path / "preprocessor", PreprocessorConf)

    def process(self, subject_data: SubjectData) -> SubjectData:
        for conf in self._filter_conigs:
            if not all(concept in subject_data.data.keys() for concept in conf.concepts):
                continue

            module_name, cls_name = conf.preprocessor.rsplit(".", 1)
            module = import_module(module_name)
            cls = getattr(module, cls_name)
            preprocessor: Preprocessor = cls(conf.concepts, **conf.params)

            subject_data = preprocessor(subject_data)

        return subject_data


# Todo: add pyAKI
