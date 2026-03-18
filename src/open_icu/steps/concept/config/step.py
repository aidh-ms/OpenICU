"""Concept step configuration models.

This module defines the configuration structure for the concept step,
including dataset path specifications and custom extraction settings.
"""
from pathlib import Path

from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig


class DatasetConfig(BaseModel):
    """Configuration for a source dataset.

    Specifies the name and file path for a source ICU dataset.

    Attributes:
        name: Name identifier for the dataset
        path: Filesystem path to the dataset directory
    """
    name: str = Field(..., description="Name of the dataset.")
    path: Path = Field(..., description="Path to the dataset.")


class CustomConfig(BaseModel):
    """Custom configuration specific to the concept step.

    Attributes:
        extraction_step: Name of the extraction step
    """

    extraction_step: str = Field(description="Name of the extraction step.")
    dataset_configs: list[DatasetConfig] = Field(
        default_factory=list, description="List of dataset-specific concept configuration paths."
    )


class ConceptStepConfig(BaseStepConfig[CustomConfig]):
    """Complete configuration for the concept step.

    Combines base step configuration with concept-specific settings.
    """

    pass
