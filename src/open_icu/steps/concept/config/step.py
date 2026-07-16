"""Concept step configuration models.

This module defines the configuration structure for the concept step,
including dataset-specific concept configuration paths and extraction step references.
"""

from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig


class DatasetConfig(BaseModel):
    """Configuration for a source dataset.

    Specifies the name, version, and file path for the concept definition of a dataset.

    Attributes:
        name: Name identifier for the dataset
        version: Version identifier for the dataset
    """

    name: str = Field(..., description="Name of the dataset.")
    version: str = Field(..., description="Version of the dataset.")


class CustomConfig(BaseModel):
    """Custom configuration specific to the concept step.

    Attributes:
        extraction_step: Name of the extraction step
        mapping_configs: List of dataset-specific concept configuration values
    """

    extraction_step: str = Field(description="Name of the extraction step.")
    mapping_configs: list[DatasetConfig] = Field(
        default_factory=list, description="List of mapping-specific concept configuration values."
    )


class ConceptStepConfig(BaseStepConfig[CustomConfig]):
    """Complete configuration for the concept step.

    Combines base step configuration with concept-specific settings.
    """

    pass
