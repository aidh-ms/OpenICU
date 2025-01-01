from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy import Connection, create_engine

from open_icu.steps.base import BaseStep
from open_icu.types.base import FHIRSchema, SubjectData
from open_icu.types.conf.concept import Concept
from open_icu.types.conf.source import SourceConfig


class PandasDatabaseMixin:
    def __init__(self, connection_uri: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._conn = self._create_conn(connection_uri)

    @lru_cache
    def _create_conn(self, connection_uri: str) -> Connection:
        engine = create_engine(connection_uri)
        return engine.connect().execution_options(stream_results=True)

    def iter_query_df(
        self,
        sql: str = "",
        chunksize: int | None = None,
        **kwargs: str,
    ) -> Iterator[DataFrame]:
        with (
            self._conn as conn,
            conn.begin(),
        ):
            for df in pd.read_sql_query(sql.format(**kwargs), conn, chunksize=chunksize):
                assert isinstance(df, pd.DataFrame)
                yield df.pipe(DataFrame)

    def get_query_df(
        self,
        sql: str = "",
        **kwargs: str,
    ) -> DataFrame:
        with (
            self._conn as conn,
            conn.begin(),
        ):
            df = pd.read_sql_query(sql.format(**kwargs), conn, chunksize=None)
            assert isinstance(df, pd.DataFrame)
            return df.pipe(DataFrame)


class Sampler(PandasDatabaseMixin):
    QUERY = """
        SELECT DISTINCT {fields}
        FROM {table}
    """

    def __init__(self, source_config: SourceConfig) -> None:
        super().__init__(source_config.connection)
        self._source_config = source_config

    def sample(self) -> Iterator[SubjectData]:
        for df in self.iter_query_df(
            sql=self.QUERY,
            chunksize=1,
            table=self._source_config.sample.table,
            fields=self._source_config.sample.field,
        ):
            yield SubjectData(
                id=str(df.loc[0, self._source_config.sample.field]), source=self._source_config.name, data={}
            )


class SourceStep(BaseStep):
    def __init__(self, config_path: Path | None = None, parent: BaseStep | None = None) -> None:
        super().__init__(config_path, parent)

        self._source_conigs: dict[str, SourceConfig] = {}
        self._concepts = []
        if config_path is not None:
            self._source_conigs = {conf.name: conf for conf in self._read_config(config_path / "sources", SourceConfig)}
            self._concepts = self._read_config(config_path / "concepts", Concept)

    def __call__(self) -> Iterator[SubjectData]:
        for source_config in self._source_conigs.values():
            sampler = Sampler(source_config)

            for subject_data in sampler.sample():
                subject_data = self.pre_process(subject_data)

                self.validate(subject_data)
                subject_data = self.process(subject_data)
                if not self.filter(subject_data):
                    continue

                subject_data = self.post_process(subject_data)

                yield subject_data

    def process(self, subject_data: SubjectData) -> SubjectData:
        source = self._source_conigs[subject_data.source]
        for concept in self._concepts:
            for concept_source in concept.sources:
                if concept_source.source != source.name:
                    continue

                module_name, cls_name = concept_source.extractor.rsplit(".", 1)
                module = import_module(module_name)
                cls = getattr(module, cls_name)
                extractor = cls(source.connection, subject_data.id, concept, concept_source)

                concept_data = extractor()
                if concept_data is None:
                    continue

                if subject_data.data.get(concept.name, None) is None:
                    subject_data.data[concept.name] = concept_data
                else:
                    subject_data.data[concept.name] = pd.concat([subject_data.data[concept.name], concept_data]).pipe(
                        DataFrame[FHIRSchema]
                    )

        return subject_data
