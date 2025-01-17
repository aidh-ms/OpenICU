from typing import Annotated

import pandas as pd
from pandera.typing import Series

from open_icu.types.fhir import CodeableConcept, FHIRFlattenSchema, FHIRObjectSchema, Period, Reference


class FHIRObjectEncounter(FHIRObjectSchema):
    """
    A class representing the FHIR Encounter schema.

    This class inherits from the FHIRObjectSchema and defines the structure of the
    FHIR Encounter schema.

    Attributes
    ----------
    type : Series[CodeableConcept]
        A pandas Series of CodeableConcepts representing the types.
    subject : Series[Reference]
        A pandas Series of References representing the subjects.
    actual_period : Series[Period]
        A pandas Series of Periods representing the actual periods.
    care_team : Series[Reference]
        A pandas Series of References representing the care teams.
    """

    type: Series[CodeableConcept]  # type: ignore[type-var]
    subject: Series[Reference]  # type: ignore[type-var]
    actual_period: Series[Period]  # type: ignore[type-var]
    care_team: Series[Reference]  # type: ignore[type-var]


class FHIREncounter(FHIRFlattenSchema):
    actual_period__start: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    actual_period__end: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    care_team: Series[str]
