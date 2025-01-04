from abc import ABC, abstractmethod
from typing import Iterator

from open_icu.steps.source.database import PandasDatabaseMixin
from open_icu.types.base import SubjectData
from open_icu.types.conf.source import SourceConfig


class Sampler(ABC):
    def __init__(self, source_config: SourceConfig) -> None:
        self._source_config = source_config

    @abstractmethod
    def sample(self) -> Iterator[SubjectData]:
        raise NotImplementedError


class SQLSampler(PandasDatabaseMixin, Sampler):
    QUERY = """
        SELECT DISTINCT {fields}
        FROM {table}
    """

    def sample(self) -> Iterator[SubjectData]:
        for df in self.iter_query_df(
            connection_uri=self._source_config.connection_uri,
            sql=self.QUERY,
            chunksize=1,
            table=self._source_config.sample.table,
            fields=self._source_config.sample.field,
        ):
            yield SubjectData(
                id=str(df.loc[0, self._source_config.sample.field]), source=self._source_config.name, data={}
            )
