"""Configuration classes for processing steps.

This module defines Pydantic models for configuring processing steps,
including dataset metadata, configuration file references, and step-specific
settings.
"""

from abc import ABCMeta

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


class BaseStepConfig[T: BaseModel](BaseConfig, metaclass=ABCMeta):
    """Abstract base configuration for processing steps.

    Extends BaseConfig with step-specific settings including workspace
    management, external configuration files, and dataset output configuration.

    Type Parameters:
        T: The type of step-specific configuration (must be a BaseModel)

    Attributes:
        overwrite: Whether to overwrite existing workspace and dataset directories
        config: Step-specific configuration object
        dataset: Dataset metadata configuration
    """

    overwrite: bool = Field(False, description="Whether to overwrite the workspace dir if it already exists.")
    config: T = Field(..., description="Additional configuration specific to the step.")
    dataset: DatasetConfig = Field(
        default_factory=DatasetConfig,
        description="Configuration for the dataset produced by the step.",
    )
