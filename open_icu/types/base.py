from pydantic import BaseModel

from open_icu.types.fhir import AbstractFHIRSchema


class SubjectData(BaseModel):
    id: str
    source: str
    data: dict[str, AbstractFHIRSchema]
