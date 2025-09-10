import pyarrow as pa  # type: ignore[valid-type]
from flexible_schema import Optional, PyArrowSchema, Required  # type: ignore[import-untyped]


class MEDSData(PyArrowSchema):
    subject_id: Required(pa.int64(), nullable=False)  # type: ignore[valid-type]
    time: pa.timestamp("us")  # type: ignore[valid-type]
    code: Required(pa.string(), nullable=False)  # type: ignore[valid-type]
    numeric_value: Optional(pa.float64())  # type: ignore[valid-type]
    text_value: Optional(pa.large_string())  # type: ignore[valid-type]


class OpenICUMEDSData(MEDSData):
    hadm_id: Optional(pa.int64())  # type: ignore[valid-type]
    stay_id: Optional(pa.int64())  # type: ignore[valid-type]
