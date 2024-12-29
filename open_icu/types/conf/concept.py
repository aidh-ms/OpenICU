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


class Concept(BaseModel):
    name: str
    description: str
    identifiers: dict[str, str]
    unit: dict[str, str]
    limits: ConceptLimits
    sources: list[ConceptSource]
