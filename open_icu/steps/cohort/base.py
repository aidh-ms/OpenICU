from importlib import import_module
from pathlib import Path

from open_icu.steps.base import BaseStep
from open_icu.steps.cohort.filters.base import CohortFilter
from open_icu.types.base import SubjectData
from open_icu.types.conf.cohort import CohortFilterConf


class CohortStep(BaseStep):
    def __init__(
        self, config_path: Path | None = None, concept_path: Path | None = None, parent: BaseStep | None = None
    ) -> None:
        super().__init__(config_path=config_path, concept_path=concept_path, parent=parent)

        self._filter_conigs: list[CohortFilterConf] = []
        if config_path is not None:
            self._filter_conigs = self._read_config(config_path / "cohort", CohortFilterConf)

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
