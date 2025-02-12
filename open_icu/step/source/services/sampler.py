from typing import Any, Iterator

from dependency_injector.wiring import Provide, inject

from open_icu.db.proto import DataFrameDatabaseExtractorProto
from open_icu.step.source.conf import SourceConfig
from open_icu.step.source.proto import SourceServiceProto
from open_icu.type.subject import SubjectData


class SubjectSampler(SourceServiceProto):
    def __init__(
        self, source_config: SourceConfig, subjects: list[str] | None = None, *args: Any, **kwargs: Any
    ) -> None:
        self._subjects = subjects or []
        self._source_config = source_config

    def __call__(self, *args: Any, **kwargs: Any) -> Iterator[SubjectData]:
        for sample in self._subjects:
            yield SubjectData(id=sample, source=self._source_config.name, data={})


class SQLSampler(SourceServiceProto):
    SQL_QUERY = """SELECT DISTINCT {field} as subject_id FROM {table}"""

    def __init__(
        self, source_config: SourceConfig, table: str = "", field: str = "", *args: Any, **kwargs: Any
    ) -> None:
        self._source_config = source_config
        self._table = table
        self._field = field

    @inject
    def __call__(
        self, df_extractor: DataFrameDatabaseExtractorProto = Provide["db_mimic"], *args: Any, **kwargs: Any
    ) -> Iterator[SubjectData]:
        for df in df_extractor.iter_df(self.SQL_QUERY, chunksize=1, table=self._table, field=self._field):
            yield SubjectData(id=str(df.loc[0, "subject_id"]), source=self._source_config.name, data={})
