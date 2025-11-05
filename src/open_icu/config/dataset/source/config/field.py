from typing import Any

from pydantic import BaseModel, Field


class FieldConfig(BaseModel):
    name: str = Field(..., description="The name of the field.")
    type: str = Field(..., description="The type of the field.")


class ConstantFieldConfig(FieldConfig):
    constant: Any = Field(..., description="The constant value for the field.")
