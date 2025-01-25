from typing import Any

from pydantic import BaseModel


class CohortFilterConfig(BaseModel):
    name: str
    description: str
    concepts: list[str]
    filter: str
    params: dict[str, Any]
