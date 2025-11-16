from pydantic import Field

from open_icu.config.dataset.source.config.table import TableConfig
from open_icu.helper.config import BaseConfig


class SourceDatasetConfig(BaseConfig):
    __key_fields__ = ("name", "version")

    name: str = Field(..., description="The name of the dataset.")
    version: str = Field(..., description="The version of the dataset.")
    tables: list[TableConfig] = Field(
        default_factory=list,
        description="List of table configurations for the dataset.",
    )
