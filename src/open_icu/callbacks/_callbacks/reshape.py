from typing import Any

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.proto import AstValue, CallbackProtocol, to_col_name
from open_icu.callbacks.registry import register_callback_cls


@register_callback_cls
class SplitExplode(CallbackProtocol):

    def __init__(
        self,
        column: AstValue,
        separator: str = ",",
        strip: bool = True,
    ) -> None:
        self.column = to_col_name(column)
        self.separator = separator
        self.strip = strip

    def __call__(self, lf: LazyFrame) -> Any:
        lf = (
            lf.with_columns(
                pl.col(self.column)
                .str.split(self.separator)
                .alias(self.column)
            )
            .explode(self.column)
        )

        if self.strip:
            lf = lf.with_columns(
                pl.col(self.column)
                .str.strip_chars()
                .alias(self.column)
            )

        return lf
