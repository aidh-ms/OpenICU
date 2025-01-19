from typing import Any

from pydantic import BaseModel


class UnitConverterConf(BaseModel):
    name: str
    description: str
    unitconverter: str
    params: dict[str, Any]
