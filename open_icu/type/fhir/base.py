from __future__ import annotations

from abc import abstractmethod

import pandera as pa
from pandera.typing import DataFrame, Series


class FHIRSchema(pa.DataFrameModel):
    """
    Abstract class for FHIR Schema.

    This class is used to define the structure of a FHIR schema. It is an abstract class
    that should be inherited by the specific FHIR schemas.
    """


class FHIRFlattenSchema(FHIRSchema):
    """
    Abstract class for FHIR Flatten Schema.

    This class is used to define the structure of a FHIR flatten schema. It is an abstract
    class that should be inherited by the specific FHIR flatten schemas.

    Attributes
    ----------
    identifier__coding : Series[str]
        A pandas Series of strings representing the identifier codings (e.g. CNOMED CT, Loinc).
    subject__reference : Series[str]
        A pandas Series of strings representing the subject references or id.
    subject__type : Series[str]
        A pandas Series of strings representing the data source of a patient.
    """

    identifier__coding: Series[str]
    subject__reference: Series[str]
    subject__type: Series[str]

    @abstractmethod
    def to_object(self) -> DataFrame[FHIRObjectSchema]:
        """
        Convert the flattened schema to an object schema.

        Returns
        -------
        FHIRObjectSchema
            The object schema.
        """
        raise NotImplementedError


class FHIRObjectSchema(FHIRSchema):
    """
    Abstract class for FHIR Object Schema.

    This class is used to define the structure of a FHIR object schema. It is an abstract
    class that should be inherited by the specific FHIR object schemas.
    """

    @abstractmethod
    def to_flatten(self) -> DataFrame[FHIRFlattenSchema]:
        """
        Convert the object schema to a flatten schema.

        Returns
        -------
        FHIRFlattenSchema
            The flatten schema.
        """
        raise NotImplementedError
