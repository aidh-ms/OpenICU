from pandera.typing import Series

from open_icu.types.fhir import CodeableReference, Dosage, FHIRObjectSchema, Period, Reference


class FHIRObjectMedicationStatement(FHIRObjectSchema):
    """
    A class representing the FHIR MedicationStatement schema.

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
