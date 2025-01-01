from typing import TypedDict

from pandera.typing import Series

from open_icu.types.fhir import (
    CodeableReference,
    FHIRSchema,
    Period,
    Quantity,
    Reference,
)


class Dosage(TypedDict):
    """
    A TypedDict representing a FHIR Dosage.

    Attributes
    ----------
    dose_quantity : Quantity
        The dose quantity of the dosage.
    rate_quantity : Quantity
        The rate quantity of the dosage.
    """

    dose_quantity: Quantity
    rate_quantity: Quantity


class FHIRMedicationStatement(FHIRSchema):
    """
    A class representing the FHIR MedicationStatement schema.

    This class inherits from the AbstractFHIRSinkSchema and defines the structure of the
    FHIR MedicationStatement schema.

    ...

    Attributes
    ----------
    _SINK_NAME : str
        The name of the sink, which is "medicationstatement" for this class.

    subject : Series[Reference]
        A pandas Series of References representing the subjects.

    effective_period : Series[Period]
        A pandas Series of Periods representing the effective periods.

    medication : Series[CodeableReference]
        A pandas Series of CodeableReferences representing the medications.

    dosage : Series[Dosage]
        A pandas Series of Dosages representing the dosages.

    """

    _SINK_NAME = "medicationstatement"

    subject: Series[Reference]  # type: ignore[type-var]
    effective_period: Series[Period]  # type: ignore[type-var]
    medication: Series[CodeableReference]  # type: ignore[type-var]
    dosage: Series[Dosage]  # type: ignore[type-var]
