from pathlib import Path

from open_icu.conf import load_yaml_configs
from open_icu.step.base import BaseStep
from open_icu.step.preprocessor.conf import PreprocessorConfig
from open_icu.step.proto import StepProto
from open_icu.type.subject import SubjectData


class SubjectPreprocessingStep(BaseStep):
    """
    A step that extracts subjects from a source.

    Parameters
    ----------
    preprocessor_configs : Path | list[PreprocessorConfig]
        The preprocessor configurations
    fail_silently : bool, default: False
        Whether to fail silently or not.
    parent : StepProto, default: None
        The parent step.
    """

    def __init__(
        self,
        preprocessor_configs: Path | list[PreprocessorConfig],
        fail_silently: bool = False,
        parent: StepProto | None = None,
    ) -> None:
        super().__init__(fail_silently, parent)

        _preprocessor_configs: list[PreprocessorConfig] = []
        if isinstance(preprocessor_configs, list):
            _preprocessor_configs = preprocessor_configs
        elif isinstance(preprocessor_configs, Path):
            _preprocessor_configs = load_yaml_configs(preprocessor_configs, PreprocessorConfig)

        self._preprocessor_configs = sorted(_preprocessor_configs, key=lambda c: c.order)

    def process(self, subject_data: SubjectData) -> SubjectData:
        for config in self._preprocessor_configs:
            subject_data = config.service(subject_data)

        return subject_data
