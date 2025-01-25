from typing import Any

from pydantic import BaseModel


class SinkConfig(BaseModel):
    name: str
    description: str
    sink: str
    params: dict[str, Any]
