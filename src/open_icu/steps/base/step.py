from abc import ABC, ABCMeta, abstractmethod
from typing import Any, cast

from open_icu.config.base import BaseConfig
from open_icu.config.registery import BaseConfigRegistery, load_config
from open_icu.pipeline.context import PipelineContext
from open_icu.steps.base.config import BaseStepConfig, ConfigurableBaseStepConfig
from open_icu.utils.type import get_generic_type

# create workspace
# safe config

class BaseStep[T: BaseStepConfig](ABC):
    def __init__(self, context: PipelineContext, config: dict[str, Any]) -> None:
        self._context = context
        self._config = self._config_type(**config)

    @property
    def _config_type(self) -> type[T]:
        t = get_generic_type(self.__class__)
        return cast(type[T], t)

    @abstractmethod
    def run(self) -> None:
        pass


class ConfigurableBaseStep[PCT: ConfigurableBaseStepConfig, SCT: BaseConfig](BaseStep[PCT], metaclass=ABCMeta):
    def __init__(self, context: PipelineContext, config: dict[str, Any]) -> None:
        super().__init__(context, config)
        self._load_subconfig()

    @property
    def _subconfig_type(self) -> type[SCT]:
        t = get_generic_type(self.__class__, 1)
        return cast(type[SCT], t)

    def _load_subconfig(self) -> None:
        class _Registry(BaseConfigRegistery[self._subconfig_type]):  # type: ignore[invalid-type-form]
            pass

        self._config_registery = _Registry()

        for sub_config in self._config.files:
            configs = load_config(sub_config.path, self._subconfig_type)
            filtered_configs = sub_config.filter(configs)
            print(filtered_configs)
            for cfg in filtered_configs:
                self._config_registery.register(cfg, overwrite=sub_config.overwrite)

        self._config_registery.save(self._context.project.configs_path)
