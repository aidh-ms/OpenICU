from typing import Annotated

import pandas as pd
from pandera.typing import Series

from open_icu.types.fhir import (
    CodeableConcept,
    FHIRFlattenSchema,
    FHIRObjectSchema,
    Quantity,
    Reference,
)


class FHIRObjectObservation(FHIRObjectSchema):
    """
    A class representing the FHIR Observation schema.

    This class inherits from the FHIRObjectSchema and defines the structure of the
    FHIR Observation schema.

    Attributes
    ----------
    code : Series[CodeableConcept]
        A pandas Series of CodeableConcepts representing the codes.
    subject : Series[Reference]
        A pandas Series of References representing the subjects.
    effective_date_time : Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
        A pandas Series of datetime objects representing the effective dates and times.
    value_quantity : Series[Quantity]
        A pandas Series of Quantities representing the value quantities.
    """

    code: Series[CodeableConcept]  # type: ignore[type-var]
    subject: Series[Reference]  # type: ignore[type-var]
    effective_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    value_quantity: Series[Quantity]  # type: ignore[type-var]


class FHIRObservation(FHIRFlattenSchema):
    effective_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    value_quantity__value: Series[float | int | str]
    value_quantity__unit: Series[str]
