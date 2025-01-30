from abc import ABC, abstractmethod
from typing import Iterator

from open_icu.steps.source.database import PandasDatabaseMixin
from open_icu.types.base import SubjectData
from open_icu.types.conf.source import SourceConfig


class Sampler(ABC):
    """
    A base class for samplers that generate SubjectData instances.

    Parameters
    ----------
    source_config : SourceConfig
        The source configuration object.
    """

    def __init__(self, source_config: SourceConfig) -> None:
        self._source_config = source_config

    @abstractmethod
    def sample(self) -> Iterator[SubjectData]:
        """
        An abstract method that generates SubjectData instances.

        Yields
        ------
        SubjectData
            A data object containing the subject ID, source name, and any additional data.
        """
        raise NotImplementedError


class SamplesSampler(Sampler):
    """
    A Sampler that generates SubjectData instances from a list of sample IDs
    contained in the configuration.

    Parameters
    ----------
    source_config : SourceConfig
        The source configuration object.
    """

    def sample(self) -> Iterator[SubjectData]:
        """
        A method that generates SubjectData instances from a list of sample IDs.

        Yields
        ------
        SubjectData
            A data object containing the subject ID, source name, and any additional data.
        """
        for sample in self._source_config.sample.samples:
            yield SubjectData(id=sample, source=self._source_config.name, data={})


class SQLSampler(PandasDatabaseMixin, Sampler):
    """
    A Sampler that generates SubjectData instances from a SQL query.

    Attributes
    ----------
    QUERY : str
        The SQL query used to get the subject IDs.

    Parameters
    ----------
    source_config : SourceConfig
        The source configuration object.
    """

    QUERY = """
        SELECT DISTINCT {field} as subject_id
        FROM {table}
    """

    def sample(self) -> Iterator[SubjectData]:
        """
        A method that generates SubjectData instances from a SQL query.

        Yields
        ------
        SubjectData
            A data object containing the subject ID, source name, and any additional data.
        """
        for df in self.iter_query_df(
            connection_uri=self._source_config.connection_uri,
            sql=self.QUERY,
            chunksize=1,
            **self._source_config.sample.params,
        ):
            yield SubjectData(id=str(df.loc[0, "subject_id"]), source=self._source_config.name, data={})
