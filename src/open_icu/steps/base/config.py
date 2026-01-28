"""Configuration classes for processing steps.

This module defines Pydantic models for configuring processing steps,
including dataset metadata, configuration file references, and step-specific
settings.
"""

from abc import ABCMeta
from pathlib import Path

from pydantic import BaseModel, Field

from open_icu.config.base import BaseConfig


class DatasetConfig(BaseModel):
    """Configuration for dataset metadata.

    Specifies metadata to be associated with the output dataset produced
    by a processing step.

    Attributes:
        metadata: Dictionary of metadata key-value pairs
    """
    metadata: dict[str, str] = Field(
        default_factory=dict,
        description="Metadata to be associated with the dataset.",
    )


class ConfigFileConfig(BaseModel):
    """Configuration for loading external configuration files.

    Specifies how to load configurations from YAML files, including
    filtering by name and overwrite behavior.

    Attributes:
        path: Path to the configuration file or directory
        overwrite: Whether to overwrite existing configurations with same identifier
        includes: If specified, only load configurations with these names
        excludes: If specified, skip configurations with these names
    """
    __open_icu_config_type__: str = "config_file"

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
    """Abstract base configuration for processing steps.

    Extends BaseConfig with step-specific settings including workspace
    management, external configuration files, and dataset output configuration.

    Type Parameters:
        T: The type of step-specific configuration (must be a BaseModel)

    Attributes:
        overwrite: Whether to overwrite existing workspace and dataset directories
        config_files: List of external configuration files to load
        config: Step-specific configuration object
        dataset: Dataset metadata configuration
    """

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
