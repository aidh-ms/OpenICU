from pandera.typing import DataFrame
from pydantic import BaseModel

from open_icu.type.fhir import FHIRSchema


class SubjectData(BaseModel):
    """
    Subject data model.

    Attributes
    ----------
    id: str
        Subject identifier.
    source: str
        Source of the data.
    data: dict[str, DataFrame[FHIRSchema]]
        Dataframes of Concept data extracted from the source for a subject.
    """

    id: str
    source: str
    data: dict[str, DataFrame[FHIRSchema]]
