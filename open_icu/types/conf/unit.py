from typing import Any

from pydantic import BaseModel


class UnitConverterConfig(BaseModel):
    name: str
    description: str
    unitconverter: str
    base_unit: str
    params: dict[str, Any]
