"""Field configuration models for table columns.

This module defines configurations for table column definitions including
type specifications and constant value columns.
"""

from typing import Any

from polars.datatypes import DataTypeClass
from pydantic import BaseModel, ConfigDict, Field, computed_field

from open_icu.steps.extraction.config.dtype import DTYPES


class ColumnConfig(BaseModel):
    """Configuration for a table column.

    Attributes:
        name: Name of the column
        type: String type name (must be in DTYPES mapping)
        params: Additional parameters for type conversion
        dtype: Computed Polars data type
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(..., description="The name of the column.")
    type: str = Field(..., description="The type of the column.")
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional parameters for the column."
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def dtype(self) -> DataTypeClass:
        return DTYPES[self.type]


class ConstantColumnConfig(ColumnConfig):
    """Configuration for a column with a constant value.

    Used to add columns with constant values to tables during extraction.

    Attributes:
        constant: The constant value to use for all rows
    """
    constant: Any = Field(..., description="The constant value for the column.")
