from typing import Annotated

import pandas as pd
from pandera.typing import Series

from open_icu.types.fhir.base import FHIRFlattenSchema, FHIRObjectSchema
from open_icu.types.fhir.types import (
    CodeableConcept,
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
    """
    This class represents the FHIR Observation schema.

    This class inherits from the FHIRFlattenSchema and defines the structure of the
    FHIR Observation schema.

    Attributes
    ----------
    identifier__coding : Series[str]
        A pandas Series of strings representing the identifier codings (e.g. CNOMED CT, Loinc).
    subject__reference : Series[str]
        A pandas Series of strings representing the subject references or id.
    subject__type : Series[str]
        A pandas Series of strings representing the data source of a patient.
    effective_date_time : Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
        A pandas Series of datetime objects representing the effective dates and times.
    value_quantity__unit : Series[str]
        A pandas Series of units for the quantity.
    """

    effective_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    value_quantity__unit: Series[str]


class FHIRNumericObservation(FHIRObservation):
    """
    This class represents the FHIR Observation schema.

    This class inherits from the FHIRFlattenSchema and defines the structure of the
    FHIR Observation schema.

    Attributes
    ----------
    identifier__coding : Series[str]
        A pandas Series of strings representing the identifier codings (e.g. CNOMED CT, Loinc).
    subject__reference : Series[str]
        A pandas Series of strings representing the subject references or id.
    subject__type : Series[str]
        A pandas Series of strings representing the data source of a patient.
    effective_date_time : Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
        A pandas Series of datetime objects representing the effective dates and times.
    value_quantity__value : Series[float]
        A pandas Series of values for the quantity.
    value_quantity__unit : Series[str]
        A pandas Series of units for the quantity.
    """

    value_quantity__value: Series[float]


class FHIRTextObservation(FHIRObservation):
    """
    This class represents the FHIR Observation schema.

    This class inherits from the FHIRFlattenSchema and defines the structure of the
    FHIR Observation schema.

    Attributes
    ----------
    identifier__coding : Series[str]
        A pandas Series of strings representing the identifier codings (e.g. CNOMED CT, Loinc).
    subject__reference : Series[str]
        A pandas Series of strings representing the subject references or id.
    subject__type : Series[str]
        A pandas Series of strings representing the data source of a patient.
    effective_date_time : Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
        A pandas Series of datetime objects representing the effective dates and times.
    value_quantity__value : Series[ str]
        A pandas Series of values for the quantity.
    value_quantity__unit : Series[str]
        A pandas Series of units for the quantity.
    """

    value_quantity__value: Series[str]
