import polars as pl

DTYPES = {
    "str": pl.String,
    "string": pl.String,
    "datetime": pl.String,

    "int": pl.Int64,
    "int8": pl.Int8,
    "int16": pl.Int16,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "uint8": pl.UInt8,
    "uint16": pl.UInt16,
    "uint32": pl.UInt32,
    "uint64": pl.UInt64,

    "float": pl.Float32,
    "float32": pl.Float32,
    "float64": pl.Float64,
    "decimal": pl.Decimal,

    "bool": pl.Boolean,
    "boolean": pl.Boolean,
}
