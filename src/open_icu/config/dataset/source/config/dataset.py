from typing import Any, Dict, List
from pydantic import Field
from abc import ABCMeta

from open_icu.config.dataset.source.config.table import TableConfig
from open_icu.helper.config import BaseConfig

import json

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
            "tables": [table.to_dict() for table in self.tables]
        }  
    
    def summary(self) -> Dict[str, Any] | str | List[Any]:
        return {
            "name": self.name,
            "version": self.version,
            "tables_length": len(self.tables),
            "table_summaries": [table.summary() for table in self.tables],
        }
    
    def __repr__(self) -> str:
        return json.dumps(self.summary(), indent=2)

