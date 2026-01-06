from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig


class DatasetConfig(BaseModel):
    name: str = Field(..., description="Name of the dataset.")
    path: str = Field(..., description="Path to the dataset.")


class CustomConfig(BaseModel):
    data: list[DatasetConfig] = Field(
        default_factory=list, description="List of datasets to be extracted."
    )


class ExtractionConfig(BaseStepConfig[CustomConfig]):
    pass
