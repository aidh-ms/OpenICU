from typing import Annotated

import pandas as pd
from pandera.typing import Series

from open_icu.types.fhir import (
    CodeableReference,
    FHIRSchema,
    Reference,
)


class FHIRDeviceUsage(FHIRSchema):
    """
    A class representing the FHIR DeviceUsage schema.

    This class inherits from the AbstractFHIRSinkSchema and defines the structure of the
    FHIR DeviceUsage schema.

    ...

    Attributes
    ----------
    _SINK_NAME : str
        The name of the sink, which is "deviceusage" for this class.

    patient : Series[Reference]
        A pandas Series of References representing the patients.

    timing_date_time : Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
        A pandas Series of datetime objects representing the timing dates and times.

    device : Series[CodeableReference]
        A pandas Series of CodeableReferences representing the devices.

    """

    _SINK_NAME = "deviceusage"

    patient: Series[Reference]  # type: ignore[type-var]
    timing_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    device: Series[CodeableReference]  # type: ignore[type-var]
