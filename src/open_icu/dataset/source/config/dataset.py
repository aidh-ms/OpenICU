from pydantic import BaseModel, Field

from open_icu.dataset.source.config.table import TableConfig


class SourceDatasetConfig(BaseModel):
    name: str = Field(..., description="The name of the dataset.")
    version: str = Field(..., description="The version of the dataset.")
    tables: list[TableConfig] = Field(
        default_factory=list,
        description="List of table configurations for the dataset.",
    )
