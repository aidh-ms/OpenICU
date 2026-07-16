"""Extraction step configuration models.

This module defines the configuration structure for the extraction step,
including dataset path specifications and custom extraction settings.
"""

from pathlib import Path

from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig


class DatasetConfig(BaseModel):
    """Configuration for a source dataset.

    Specifies the name and file path for a source ICU dataset.

    Attributes:
        name: Name identifier for the dataset. Must match the dataset name used
            by table configurations.
        version: Version string for the dataset.
        path: Filesystem path to the dataset directory.
        includes: Optional list of sections to include from the dataset config file.
        excludes: Optional list of sections to exclude from the dataset config file.
    """

    name: str = Field(..., description="Name of the dataset.")
    version: str = Field(..., description="Version of the dataset.")
    path: Path = Field(..., description="Path to the dataset.")
    includes: list[str] | None = Field(default=None, description="List of sections to include from the config file.")
    excludes: list[str] | None = Field(default=None, description="List of sections to exclude from the config file.")


class CustomConfig(BaseModel):
    """Custom configuration specific to the extraction step.

    Attributes:
        data: List of source datasets to process
    """

    data: list[DatasetConfig] = Field(default_factory=list, description="List of datasets to be extracted.")


class ExtractionStepConfig(BaseStepConfig[CustomConfig]):
    """Complete configuration for the extraction step.

    Combines base step configuration with extraction-specific settings.
    """

    pass
