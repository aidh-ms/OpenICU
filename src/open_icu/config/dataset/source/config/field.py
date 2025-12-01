from typing import Any

from polars.datatypes import DataTypeClass
from pydantic import BaseModel, Field, computed_field

from open_icu.config.dataset.source.config.dtype import DTYPES


class FieldConfig(BaseModel):
    name: str = Field(..., description="The name of the field.")
    type: str = Field(..., description="The type of the field.")
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional parameters for the field."
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def dtype(self) -> DataTypeClass:
        return DTYPES[self.type]


class ConstantFieldConfig(FieldConfig):
    constant: Any = Field(..., description="The constant value for the field.")
