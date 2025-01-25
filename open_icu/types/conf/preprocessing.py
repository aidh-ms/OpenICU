from typing import Any

from pydantic import BaseModel


class PreprocessorConfig(BaseModel):
    name: str
    description: str
    concepts: list[str]
    preprocessor: str
    params: dict[str, Any]
