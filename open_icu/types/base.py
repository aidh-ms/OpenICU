from pandera.typing import DataFrame
from pydantic import BaseModel

from open_icu.types.fhir import FHIRSchema


class SubjectData(BaseModel):
    id: str
    source: str
    data: dict[str, DataFrame[FHIRSchema]]
