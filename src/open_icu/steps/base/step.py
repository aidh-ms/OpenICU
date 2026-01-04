import shutil
from abc import ABC, ABCMeta, abstractmethod
from pathlib import Path
from typing import Any, cast

from open_icu.config.base import BaseConfig
from open_icu.config.registery import BaseConfigRegistery
from open_icu.pipeline.context import PipelineContext
from open_icu.steps.base.config import BaseStepConfig, ConfigurableBaseStepConfig
from open_icu.utils.type import get_generic_type


class BaseStep[T: BaseStepConfig](ABC):
    def __init__(self, context: PipelineContext, config: dict[str, Any]) -> None:
        self._context = context
        self._config = self._config_type(**config)

    @property
    def _config_type(self) -> type[T]:
        t = get_generic_type(self.__class__)
        return cast(type[T], t)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def setup(self) -> None:
        pass

    def teardown(self) -> None:
        pass

    def pre_run(self) -> None:
        name = self._config.workspace.name
        if name is None:
            name = self.name

        self._workspace = self._context.project.add_workspace_dir(
            name=name,
            overwrite=self._config.workspace.overwrite,
        )

    def post_run(self) -> None:
        pass

    @abstractmethod
    def run(self) -> None:
        pass


class ConfigurableBaseStep[PCT: ConfigurableBaseStepConfig, SCT: BaseConfig](BaseStep[PCT], metaclass=ABCMeta):
    def __init__(self, context: PipelineContext, config: dict[str, Any]) -> None:
        super().__init__(context, config)

        self._config_registery = self._create_registry()

    @property
    def _subconfig_type(self) -> type[SCT]:
        t = get_generic_type(self.__class__, 1)
        return cast(type[SCT], t)

    @property
    def registery(self) -> BaseConfigRegistery[SCT]:
        return self._config_registery

    def _create_registry(self) -> BaseConfigRegistery[SCT]:
        class _Registry(BaseConfigRegistery[self._subconfig_type]):  # type: ignore[invalid-type-form]
            pass
        return _Registry()

    def setup(self) -> None:
        for sub_config in self._config.files:
            for file_path in sub_config.path.rglob("*.*"):
                if (
                    not file_path.is_file()
                    or file_path.suffix.lower() not in {".yml", ".yaml"}
                ):
                    continue

                try:
                    config = self._subconfig_type.load(file_path)
                except Exception:
                    continue

                if not sub_config.matches(config):
                    continue

                self._config_registery.register(config, overwrite=sub_config.overwrite)

                project_config_path = Path(self._context.project.configs_path, *config.identifier_tuple).with_suffix(".yml")
                project_config_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(file_path, project_config_path)
