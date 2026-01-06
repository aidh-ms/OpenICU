from abc import ABCMeta
from pathlib import Path

from pydantic import BaseModel, Field

from open_icu.config.base import BaseConfig


class DatasetConfig(BaseModel):
    metadata: dict[str, str] = Field(
        default_factory=dict,
        description="Metadata to be associated with the dataset.",
    )


class ConfigFileConfig(BaseModel):
    path: Path = Field(..., description="The path to the configuration file.")
    overwrite: bool = Field(
        False, description="Whether to overwrite the configuration file if it already exists."
    )
    includes: list[str] | None = Field(
        default=None, description="List of sections to include from the config file."
    )
    excludes: list[str] | None = Field(
        default=None, description="List of sections to exclude from the config file."
    )


class BaseStepConfig[T: BaseModel](BaseConfig, metaclass=ABCMeta):
    overwrite: bool = Field(
        False, description="Whether to overwrite the workspace dir if it already exists."
    )
    config_files: list[ConfigFileConfig] = Field(
        default_factory=list,
        description="List of configuration files to be used by the step.",
    )
    config: T  = Field(..., description="Additional configuration specific to the step.")
    dataset: DatasetConfig = Field(
        default_factory=DatasetConfig,
        description="Configuration for the dataset produced by the step.",
    )
