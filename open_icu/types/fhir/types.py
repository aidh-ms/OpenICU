from enum import StrEnum, auto
from typing import Annotated, TypedDict

import pandas as pd
from pydantic import PlainValidator


class Reference(TypedDict):
    """
    A TypedDict representing a FHIR Reference.

    Attributes
    ----------
    reference : str
        The actual reference to the resource.
    type : str
        The type of the referenced resource.
    """

    reference: str
    type: str


class Quantity(TypedDict):
    """
    A TypedDict representing a FHIR Quantity.

    Attributes
    ----------
    value : float
        The numerical value of the quantity.
    unit : str
        The unit of the quantity.
    """

    value: float | int | str
    unit: str


class Period(TypedDict):
    """
    A TypedDict representing a FHIR Period.

    Attributes
    ----------
    start : pd.Timestamp
        The start of the period.
    end : pd.Timestamp
        The end of the period.
    """

    start: Annotated[pd.Timestamp, PlainValidator(lambda x: pd.Timestamp(x))]
    end: Annotated[pd.Timestamp, PlainValidator(lambda x: pd.Timestamp(x))]


class Coding(TypedDict):
    """
    A TypedDict representing a FHIR Coding.

    Attributes
    ----------
    code : str
        The code of the coding.
    system : str
        The system of the coding.
    """

    code: str
    system: str


class CodeableConcept(TypedDict):
    """
    A TypedDict representing a FHIR CodeableConcept.

    Attributes
    ----------
    coding : Coding
        The coding of the CodeableConcept.
    """

    coding: list[Coding]


class CodeableReference(TypedDict):
    """
    A TypedDict representing a FHIR CodeableReference.

    Attributes
    ----------
    concept : CodeableConcept
        The CodeableConcept of the CodeableReference.
    """

    concept: CodeableConcept


class Stage(TypedDict):
    """
    A TypedDict representing a FHIR Stage.

    Attributes
    ----------
    summary : CodeableConcept
        The summary of the stage.
    assessment : list[Reference]
        The assessments of the stage.
    """

    assessment: list[Reference]


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


class StatusCodes(StrEnum):
    PLANNED = auto()
    IN_PROGRESS = auto()
    ON_HOLD = auto()
    DISCHARGED = auto()
    COMPLETED = auto()
    CANCELLED = auto()
    DISCONTINUED = auto()
