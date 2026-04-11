from pathlib import Path

from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig


class DatasetConfig(BaseModel):
    """Configuration for a source dataset.

    Specifies the name and file path for to the sharding definition of a dataset.

    Attributes:
        name: Name identifier for the dataset
        path: Filesystem path to the dataset directory
    """
    name: str = Field(..., description="Name of the dataset.")
    path: Path = Field(..., description="Path to the dataset.")


class CustomConfig(BaseModel):
    """Custom configuration specific to the sharding step.

    Attributes:
        concept_step: Name of the concept step
    """

    concept_step: str = Field(description="Name of the concept step.")
    dataset_configs: list[DatasetConfig] = Field(
        default_factory=list, description="List of dataset-specific sharding configuration paths."
    )

class ShardingStepConfig(BaseStepConfig[CustomConfig]):
    """Complete configuration for the sharding step.

    Combines base step configuration with sharding-specific settings.
    """

    pass
