from typing import Annotated

import pandas as pd
from pandera.typing import Series

from open_icu.types.fhir import CodeableReference, FHIRFlattenSchema, FHIRObjectSchema, Reference, StatusCodes


class FHIRObjectDeviceUsage(FHIRObjectSchema):
    """
    A class representing the FHIR DeviceUsage schema.

    This class inherits from the FHIRObjectSchema and defines the structure of the
    FHIR DeviceUsage schema.

    Attributes
    ----------
    patient : Series[Reference]
        A pandas Series of References representing the patients.
    timing_date_time : Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
        A pandas Series of datetime objects representing the timing dates and times.
    device : Series[CodeableReference]
        A pandas Series of CodeableReferences representing the devices.
    status : Series[StatusCodes]
        A pandas Series of StatusCodes representing the status of the device usage.
    """

    patient: Series[Reference]  # type: ignore[type-var]
    timing_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    device: Series[CodeableReference]  # type: ignore[type-var]
    status: Series[StatusCodes]


class FHIRDeviceUsage(FHIRFlattenSchema):
    timing_date_time: Series[Annotated[pd.DatetimeTZDtype, "ns", "utc"]]
    status: Series[StatusCodes]
