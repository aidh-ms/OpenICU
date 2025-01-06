from typing import Any

from pydantic import BaseModel


class CohortFilterConf(BaseModel):
    name: str
    description: str
    concepts: list[str]
    filter: str
    params: dict[str, Any]
