from importlib import import_module
from pathlib import Path

from open_icu.steps.base import BaseStep
from open_icu.steps.cohort.filters.base import CohortFilter
from open_icu.types.base import SubjectData
from open_icu.types.conf.cohort import CohortFilterConfig
from open_icu.types.conf.concept import ConceptConfig


class CohortStep(BaseStep[CohortFilterConfig]):
    """
    A step that filters subjects based on their data.

    Parameters
    ----------
    configs : Path | list[CohortFilterConfig] | None
        The path to the configuration files or a list of configurations.
    concept_configs : Path | list[ConceptConfig] | None
        The path to the concept configuration files or a list of configurations.
    parent : BaseStep | None
        The parent step.
    """

    def __init__(
        self,
        configs: Path | list[CohortFilterConfig] | None = None,
        concept_configs: Path | list[ConceptConfig] | None = None,
        parent: BaseStep | None = None,
    ) -> None:
        super().__init__(configs=configs, concept_configs=concept_configs, parent=parent)

        self._filter_configs: list[CohortFilterConfig] = []
        if isinstance(configs, list):
            self._filter_configs = configs
        elif self._config_path is not None:
            self._filter_configs = self._read_config(self._config_path / "cohort", CohortFilterConfig)

    def filter(self, subject_data: SubjectData) -> bool:
        """
        Filter out subjects based on their data.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to filter.

        Returns
        -------
        bool
            Whether the subject should be included or not.
        """
        for conf in self._filter_configs:
            print(conf)
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
