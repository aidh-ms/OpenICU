import pyarrow as pa
from flexible_schema import Optional, PyArrowSchema, Required


class MEDSData(PyArrowSchema):
    subject_id: Required(pa.int64(), nullable=False)
    time: pa.timestamp("us")
    code: Required(pa.string(), nullable=False)
    numeric_value: Optional(pa.float64())
    text_value: Optional(pa.large_string())


class OpenICUMEDSData(MEDSData):
    hadm_id: Optional(pa.int64())
    stay_id: Optional(pa.int64())

