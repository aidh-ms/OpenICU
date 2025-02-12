from typing import Any, Iterator, Protocol

from pandera.typing import DataFrame


class DataFrameDatabaseExtractorProto(Protocol):
    """
    A protocol for a database extractor class that returns pandas DataFrames.
    """

    def __init__(self, conncetion_uri: str) -> None:
        ...

    def iter_df(self, query: str, chunksize: int | None = None, *args: Any, **kwargs: Any) -> Iterator[DataFrame]:
        ...

    def get_df(self, query: str, *args: Any, **kwargs: Any) -> DataFrame:
        ...
