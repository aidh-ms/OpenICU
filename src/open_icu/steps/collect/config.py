

from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig


class DatasetConfig(BaseModel):
    name: str = Field(..., description="The name of the MEDS dataset.")
    overwrite: bool = Field(
        False,
        description="Whether to overwrite the existing MEDS dataset if it exists.",
    )
    link: bool = Field(
        False,
        description="Whether to copy or symlink the files to the dataset.",
    )
    metadata: dict[str, str] = Field(
        default_factory=dict,
        description="Metadata to add to the collected dataset.",
    )


class CollectingStepConfig(BaseModel):
    name: str = Field(..., description="The name of the collection step.")
    overwite: bool = Field(
        True,
        description="Whether to overwrite the existing collection step workspace if it exists.",
    )


class CollectionStepConfig(BaseStepConfig):
    dataset: DatasetConfig = Field(
        ...,
        description="Configuration for the MEDS dataset to collect data into.",
    )
    collecting: list[CollectingStepConfig] = Field(
        ...,
        description="Configuration for the data collecting step.",
    )
