from abc import ABC, ABCMeta, abstractmethod
from typing import Any, cast

from open_icu.pipeline.context import PipelineContext
from open_icu.steps.base.config import BaseStepConfig
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

    @abstractmethod
    def run(self) -> None:
        pass


class BaseConfigStep[T: BaseStepConfig](BaseStep[T], metaclass=ABCMeta):
    pass


# copy config
# dataset linking from workspace
# add ref to state
# write to wrorkspace
# logging
# config step
