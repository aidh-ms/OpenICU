from pathlib import Path

from pydantic import Field

from open_icu.steps.base.config import ConfigurableBaseStepConfig, SubStepConfig


class DatasetConfig(SubStepConfig):
    dataset_path: Path = Field(..., description="The path to the dataset.")


class ExtractionStepConfig(ConfigurableBaseStepConfig):
    files: list[DatasetConfig] = Field(
        default_factory=list, description="A list of dataset configurations."
    )
