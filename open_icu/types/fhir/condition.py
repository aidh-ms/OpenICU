from typing import Annotated

import pandas as pd
from pandera.typing import Series

from open_icu.types.fhir import (
    CodeableConcept,
    FHIRFlattenSchema,
    FHIRObjectSchema,
    Reference,
    Stage,
)


class FHIRObjectCondition(FHIRObjectSchema):
    """
    A class representing the FHIR Condition schema.

    This class inherits from the FHIRObjectSchema and defines the structure of the
    FHIR Condition schema.

    Attributes
    ----------
    code : Series[CodeableConcept]
        A pandas Series of CodeableConcepts representing the codes.
    subject : Series[Reference]
        A pandas Series of References representing the subjects.
    onset_date_time : Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
        A pandas Series of datetime objects representing the date and time of a condition.
    stage : Series[Stage]
        A pandas Series of Stage representing the stage of a condition.
    """

    code: Series[CodeableConcept]  # type: ignore[type-var]
    subject: Series[Reference]  # type: ignore[type-var]
    onset_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    stage: Series[Stage]  # type: ignore[type-var]


class FHIRObservation(FHIRFlattenSchema):
    onset_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    stage__assessment: Series[str]
