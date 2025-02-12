from typing import Any, Iterator, Protocol

from pandera.typing import DataFrame


class DataFrameDatabaseExtractorProto(Protocol):
    def __init__(self, conncetion_uri: str) -> None:
        ...

    def iter_df(self, query: str, chunksize: int | None = None, *args: Any, **kwargs: Any) -> Iterator[DataFrame]:
        ...

    def get_df(self, query: str, *args: Any, **kwargs: Any) -> DataFrame:
        ...
