from pathlib import Path
from typing import Iterator

import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy import create_engine

from open_icu.steps.base import BaseStep
from open_icu.types.base import SubjectData
from open_icu.types.conf.concept import Concept
from open_icu.types.conf.source import SourceConfig


class PandasDatabaseMixin:
    def __init__(self, connection_uri: str) -> None:
        engine = create_engine(connection_uri)
        self._conn = engine.connect().execution_options(stream_results=True)

    def query_df(
        self,
        query: str = "",
        chunksize: int | None = None,
        **kwargs: str,
    ) -> Iterator[DataFrame]:
        with (
            self._conn as conn,
            conn.begin(),
        ):
            for df in pd.read_sql_query(query.format(**kwargs), conn, chunksize=chunksize):
                assert isinstance(df, pd.DataFrame)
                yield df.pipe(DataFrame)


class Sampler(PandasDatabaseMixin):
    QUERY = """
        SELECT DISTINCT {fields}
        FROM {table}
    """

    def sample(self, source_config: SourceConfig) -> Iterator[SubjectData]:
        for df in self.query_df(
            query=self.QUERY,
            chunksize=1,
            table=source_config.sample.table,
            fields=source_config.sample.field,
        ):
            yield SubjectData(id=str(df.loc[0, source_config.sample.field]), source=source_config.name, data={})


class SourceStep(BaseStep):
    def __init__(self, config_path: Path | None = None, parent: BaseStep | None = None) -> None:
        super().__init__(config_path, parent)

        if config_path is not None:
            self._source_conigs = self._read_config(config_path / "sources", SourceConfig)
            self._concepts = self._read_config(config_path / "concepts", Concept)
        else:
            self._source_conigs = []
            self._concepts = []

    def __call__(self) -> Iterator[SubjectData]:
        for source_config in self._source_conigs:
            print(f"Sampling from {source_config.name}...")
            sampler = Sampler(source_config.connection)

            for subject_data in sampler.sample(source_config):
                subject_data = self.pre_process(subject_data)

                self.validate(subject_data)
                subject_data = self.process(subject_data)
                if not self.filter(subject_data):
                    continue

                subject_data = self.post_process(subject_data)

                yield subject_data

    def process(self, subject_data: SubjectData) -> SubjectData:
        return subject_data
