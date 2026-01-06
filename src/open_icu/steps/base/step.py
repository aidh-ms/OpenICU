import shutil
from abc import ABCMeta, abstractmethod
from pathlib import Path

from open_icu.config.base import BaseConfig
from open_icu.config.registery import BaseConfigRegistery
from open_icu.steps.base.config import BaseStepConfig
from open_icu.storage.project import OpenICUProject
from open_icu.storage.workspace import WorkspaceDir


class ConfigurableBaseStep[SCT: BaseStepConfig, CT: BaseConfig](metaclass=ABCMeta):
    def __init__(self, project: OpenICUProject, config: SCT, registery: BaseConfigRegistery[CT]) -> None:
        self._project = project
        self._config = config
        self._registery = registery
        self._workspace_dir = None
        self._dataset = None
        self._step_name = self._config.name.lower()

    @classmethod
    @abstractmethod
    def load(cls, project: OpenICUProject, config_path: Path) -> "ConfigurableBaseStep[SCT, CT]":
        pass

    @abstractmethod
    def extract(self) -> None:
        pass

    def run(self) -> WorkspaceDir:
        sikp = (
            not self._config.overwrite
            and (self._project.workspace_path / self._step_name).exists()
            and (self._project.datasets_path / self._step_name).exists()
        )

        self.setup_config()
        self.setup_project()
        if not sikp:
            self.extract()
            self.hooks()
            self.collect()

        assert isinstance(self._workspace_dir, WorkspaceDir)
        return self._workspace_dir

    def setup_config(self) -> None:
        for config in self._config.config_files:
            self._registery.load(
                config.path,
                overwrite=config.overwrite,
                includes=config.includes,
                excludes=config.excludes
            )

        self._registery.save(self._project.configs_path)

    def setup_project(self) -> None:
        self._workspace_dir = self._project.add_workspace_dir(
            name=self._step_name,
            overwrite=self._config.overwrite,
        )

        self._dataset = self._project.add_dataset(
            name=self._step_name,
            overwrite=self._config.overwrite,
        )

    def hooks(self) -> None:
        # TODO run hooks from registery after extraction
        pass

    def collect(self) -> None:
        if self._workspace_dir is None or self._dataset is None:
            return

        for file_path in self._workspace_dir.content:
            relative_path = file_path.relative_to(self._workspace_dir._path)
            dest_path = self._dataset.data_path / relative_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            shutil.copy(file_path, dest_path)

        self._dataset.write_metadata(self._config.dataset.metadata)
        self._dataset.write_codes()
