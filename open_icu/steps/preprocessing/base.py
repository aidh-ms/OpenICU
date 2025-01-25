from importlib import import_module
from pathlib import Path

from open_icu.steps.base import BaseStep
from open_icu.steps.preprocessing.processor.base import Preprocessor
from open_icu.types.base import SubjectData
from open_icu.types.conf.concept import ConceptConfig
from open_icu.types.conf.preprocessing import PreprocessorConfig


class SubjectPreprocessingStep(BaseStep[PreprocessorConfig]):
    def __init__(
        self,
        configs: Path | list[PreprocessorConfig] | None = None,
        concept_configs: Path | list[ConceptConfig] | None = None,
        parent: BaseStep | None = None,
    ) -> None:
        super().__init__(configs=configs, concept_configs=concept_configs, parent=parent)

        self._filter_conigs: list[PreprocessorConfig] = []
        if isinstance(configs, list):
            self._filter_conigs = configs
        elif self._config_path is not None:
            self._filter_conigs = self._read_config(self._config_path / "preprocessor", PreprocessorConfig)

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
