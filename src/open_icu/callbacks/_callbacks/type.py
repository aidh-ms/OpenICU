from typing import Optional

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, CallbackResult, to_expr
from open_icu.callbacks.registry import register_callback_cls


_DTYPE_MAP: dict[str, pl.DataType] = {
    "int8": pl.Int8,
    "int16": pl.Int16,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "uint8": pl.UInt8,
    "uint16": pl.UInt16,
    "uint32": pl.UInt32,
    "uint64": pl.UInt64,
    "float32": pl.Float32,
    "float64": pl.Float64,
    "bool": pl.Boolean,
    "boolean": pl.Boolean,
    "str": pl.String,
    "string": pl.String,
    "utf8": pl.String,
    "date": pl.Date,
    "datetime": pl.Datetime,
}


@register_callback_cls
class Cast(CallbackProtocol):
    def __init__(
        self,
        value: AstValue,
        dtype: str,
        strict: bool = False,
        output: Optional[str] = None,
    ) -> None:
        self.value = value
        self.dtype = dtype
        self.strict = strict
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        dtype_key = self.dtype.lower()

        if dtype_key not in _DTYPE_MAP:
            allowed = ", ".join(sorted(_DTYPE_MAP))
            raise ValueError(
                f"Unsupported dtype '{self.dtype}'. "
                f"Allowed dtypes are: {allowed}"
            )

        expr = to_expr(lf, self.value).cast(
            _DTYPE_MAP[dtype_key],
            strict=self.strict,
        )

        if self.output is None:
            return expr

        return expr.alias(self.output)