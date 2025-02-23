from pathlib import Path

from open_icu.conf import load_yaml_configs
from open_icu.step.base import BaseStep
from open_icu.step.cohort.conf import CohortConfig
from open_icu.step.proto import StepProto
from open_icu.type.subject import SubjectData


class CohortFilterStep(BaseStep):
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
        cohort_configs: Path | list[CohortConfig],
        fail_silently: bool = False,
        parent: StepProto | None = None,
    ) -> None:
        super().__init__(fail_silently, parent)

        self._cohort_configs: list[CohortConfig] = []
        if isinstance(cohort_configs, list):
            self._cohort_configs = cohort_configs
        elif isinstance(cohort_configs, Path):
            self._cohort_configs = load_yaml_configs(cohort_configs, CohortConfig)

    def filter(self, subject_data: SubjectData) -> bool:
        for config in self._cohort_configs:
            if config.service(subject_data):
                return True

        return False
