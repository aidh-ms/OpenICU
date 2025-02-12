from typing import Any, Iterator

from dependency_injector.containers import Container
from dependency_injector.wiring import Provide, inject

from open_icu.db.proto import DataFrameDatabaseExtractorProto
from open_icu.step.source.conf import SourceConfig
from open_icu.step.source.proto import SamplerServiceProto
from open_icu.type.subject import SubjectData


class SubjectSampler(SamplerServiceProto):
    """
    A sampler service that samples subjects from a list of subjects.

    Parameters
    ----------
    source_config : SourceConfig
        The source configuration.
    subjects : list[str], default: None
        The list of subjects to sample from.
    args : Any
        Additional arguments.
    kwargs : Any
        Additional keyword arguments.
    """

    def __init__(
        self, source_config: SourceConfig, subjects: list[str] | None = None, *args: Any, **kwargs: Any
    ) -> None:
        self._subjects = subjects or []
        self._source_config = source_config

    def __call__(self, *args: Any, **kwargs: Any) -> Iterator[SubjectData]:
        """
        A generator that yields subjects based on the list of subjects.

        Returns
        -------
        Iterator[SubjectData]
            An iterator of the subjects.
        """
        for sample in self._subjects:
            yield SubjectData(id=sample, source=self._source_config.name, data={})


class SQLSampler(SamplerServiceProto):
    """
    A sampler service that samples subjects from a SQL table.

    Parameters
    ----------
    source_config : SourceConfig
        The source configuration.
    table : str
        The table to sample from.
    field : str
        The field to sample from.
    args : Any
        Additional arguments.
    kwargs : Any
        Additional keyword arguments.
    """

    SQL_QUERY = """SELECT DISTINCT {field} as subject_id FROM {table}"""

    def __init__(
        self, source_config: SourceConfig, table: str = "", field: str = "", *args: Any, **kwargs: Any
    ) -> None:
        self._source_config = source_config
        self._table = table
        self._field = field

    @inject
    def __call__(
        self, container: Container = Provide["<container>"], *args: Any, **kwargs: Any
    ) -> Iterator[SubjectData]:
        """
        A generator that yields subjects based on the SQL table.

        Parameters
        ----------
        container : Container
            The di container containing the database extractor.
        args : Any
            Additional arguments.
        kwargs : Any
            Additional keyword arguments.

        Returns
        -------
        Iterator[SubjectData]
            An iterator of the subjects.
        """
        df_extractor: DataFrameDatabaseExtractorProto = getattr(container, f"db_{self._source_config.name}")()

        for df in df_extractor.iter_df(self.SQL_QUERY, chunksize=1, table=self._table, field=self._field):
            yield SubjectData(id=str(df.loc[0, "subject_id"]), source=self._source_config.name, data={})
