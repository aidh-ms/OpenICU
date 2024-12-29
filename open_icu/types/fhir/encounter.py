from pandera.typing import Series

from open_icu.types.fhir import (
    AbstractFHIRSchema,
    Period,
    Reference,
)


class FHIREncounter(AbstractFHIRSchema):
    """
    A class representing the FHIR Encounter schema.

    This class inherits from the AbstractFHIRSinkSchema and defines the structure of the
    FHIR Encounter schema.

    ...

    Attributes
    ----------
    _SINK_NAME : str
        The name of the sink, which is "encounter" for this class.

    subject : Series[Reference]
        A pandas Series of References representing the subjects.

    actual_period : Series[Period]
        A pandas Series of Periods representing the actual periods.

    care_team : Series[Reference]
        A pandas Series of References representing the care teams.

    """

    _SINK_NAME = "encounter"

    subject: Series[Reference]  # type: ignore[type-var]
    actual_period: Series[Period]  # type: ignore[type-var]
    care_team: Series[Reference]  # type: ignore[type-var]
