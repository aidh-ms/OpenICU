from typing import Any, Dict, List
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

    def to_dict(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "name": self.name,
            "version": self.version,
            "tables": [table.to_dict() for table in self.tables],
        }    

    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "name": self.name,
            "version": self.version,
            "table_count": len(self.tables),
        }