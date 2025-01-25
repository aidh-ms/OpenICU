from importlib import import_module
from pathlib import Path

from open_icu.steps.base import BaseStep
from open_icu.steps.cohort.filters.base import CohortFilter
from open_icu.types.base import SubjectData
from open_icu.types.conf.cohort import CohortFilterConfig
from open_icu.types.conf.concept import ConceptConfig


class CohortStep(BaseStep[CohortFilterConfig]):
    def __init__(
        self,
        configs: Path | list[CohortFilterConfig] | None = None,
        concept_configs: Path | list[ConceptConfig] | None = None,
        parent: BaseStep | None = None,
    ) -> None:
        super().__init__(configs=configs, concept_configs=concept_configs, parent=parent)

        self._filter_conigs: list[CohortFilterConfig] = []
        if isinstance(configs, list):
            self._filter_conigs = configs
        elif self._config_path is not None:
            self._filter_conigs = self._read_config(self._config_path / "cohort", CohortFilterConfig)

    def filter(self, subject_data: SubjectData) -> bool:
        for conf in self._filter_conigs:
            if not all(concept in subject_data.data.keys() for concept in conf.concepts):
                continue

            module_name, cls_name = conf.filter.rsplit(".", 1)
            module = import_module(module_name)
            cls = getattr(module, cls_name)
            filter: CohortFilter = cls(conf.concepts, **conf.params)

            if not filter(subject_data):
                continue

            return True

        return False
