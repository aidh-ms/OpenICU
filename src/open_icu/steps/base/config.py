from abc import ABCMeta
from pathlib import Path

from pydantic import BaseModel, Field

from open_icu.config.base import BaseConfig


class BaseStepConfig(BaseModel, metaclass=ABCMeta):
    pass


class ConfigFilter(BaseModel):
    name: str = Field(..., description="The name of the config.")
    version: str | None = Field(
        None, description="The version of the config, if applicable."
    )


class SubStepConfig(BaseModel):
    path: Path = Field(..., description="The path to the output file.")
    overwrite: bool = Field(
        False, description="Whether to overwrite the file if it already exists."
    )
    filters: list[ConfigFilter] = Field(
        default_factory=list, description="A list of filters to apply when selecting configs."
    )

    def filter[T: BaseConfig](self, configs: list[T]) -> list[T]:
        if not self.filters:
            return configs

        filtered_configs = []
        for config in configs:
            for config_filter in self.filters:
                if config.name != config_filter.name:
                    continue

                if config_filter.version is not None and config.version != config_filter.version:
                    continue

                filtered_configs.append(config)
        return filtered_configs


class ConfigurableBaseStepConfig(BaseStepConfig, metaclass=ABCMeta):
    files: list[SubStepConfig] = Field(
        default_factory=list, description="A list of sub-step configurations."
    )
