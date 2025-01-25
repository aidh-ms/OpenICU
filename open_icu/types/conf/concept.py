from typing import Any

from pydantic import BaseModel


class ConceptLimits(BaseModel):
    upper: str
    lower: str


class ConceptSource(BaseModel):
    source: str
    extractor: str
    unit: dict[str, str]
    params: dict[str, Any]


class ConceptConfig(BaseModel):
    name: str
    description: str
    identifiers: dict[str, str]
    unit: dict[str, str]
    limits: ConceptLimits
    sources: list[ConceptSource]

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)

        self.identifiers["open_icu"] = self.name
