from typing import Annotated

import pandas as pd
from pandera.typing import Series

from open_icu.types.fhir.base import FHIRFlattenSchema, FHIRObjectSchema
from open_icu.types.fhir.types import (
    CodeableReference,
    Dosage,
    Period,
    Reference,
)


class FHIRObjectMedicationStatement(FHIRObjectSchema):
    """
    This class represents the FHIR MedicationStatement schema.

    This class inherits from the FHIRObjectSchema and defines the structure of the
    FHIR MedicationStatement schema.

    Attributes
    ----------
    subject : Series[Reference]
        A pandas Series of References representing the subjects.
    effective_period : Series[Period]
        A pandas Series of Periods representing the effective periods.
    medication : Series[CodeableReference]
        A pandas Series of CodeableReferences representing the medications.
    dosage : Series[Dosage]
        A pandas Series of Dosages representing the dosages.
    """

    subject: Series[Reference]  # type: ignore[type-var]
    effective_period: Series[Period]  # type: ignore[type-var]
    medication: Series[CodeableReference]  # type: ignore[type-var]
    dosage: Series[Dosage]  # type: ignore[type-var]


class FHIRMedicationStatement(FHIRFlattenSchema):
    """
    This class represents the FHIR MedicationStatement schema.

    This class inherits from the FHIRFlattenSchema and defines the structure of the
    FHIR MedicationStatement schema.

    Attributes
    ----------
    identifier__coding : Series[str]
        A pandas Series of strings representing the identifier codings (e.g. CNOMED CT, Loinc) of the medication.
    subject__reference : Series[str]
        A pandas Series of strings representing the subject references or id.
    subject__type : Series[str]
        A pandas Series of strings representing the data source of a patient.
    effective_period__start : Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
        A pandas Series of datetime objects representing the start of the effective periods.
    effective_period__end : Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
        A pandas Series of datetime objects representing the end of the effective periods.
    dosage__dose_quantity__value : Series[float | int | str]
        A pandas Series of values for the dose quantity.
    dosage__dose_quantity__unit : Series[str]
        A pandas Series of units for the dose quantity.
    dosage__rate_quantity__value : Series[float | int | str]
        A pandas Series of values for the rate quantity.
    dosage__rate_quantity__unit : Series[str]
        A pandas Series of units for the rate quantity.
    """

    effective_period__start: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    effective_period__end: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    dosage__dose_quantity__value: Series[float | int | str]
    dosage__dose_quantity__unit: Series[str]
    dosage__rate_quantity__value: Series[float | int | str]
    dosage__rate_quantity__unit: Series[str]
