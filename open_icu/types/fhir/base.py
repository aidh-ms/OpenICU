from __future__ import annotations

from abc import abstractmethod

import pandera as pa
from pandera.typing import DataFrame


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
    """

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
