from typing import Any, Iterator

import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy import create_engine

from open_icu.data.proto import IDataFrameDatabaseExtractor


class SQLDataFrameDatabaseExtractor(IDataFrameDatabaseExtractor):
    """
    A class for querying databases and returning pandas DataFrames.

    Parameters
    ----------
    conncetion_uri : str
        The connection URI for the database.
    """

    def __init__(self, conncetion_uri: str) -> None:
        self._engine = create_engine(conncetion_uri)

    def iter_df(
        self,
        query: str,
        chunksize: int,
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[DataFrame]:
        """
        A generator function that yields pandas DataFrames from a SQL query.

        Parameters
        ----------
        query : str
            The SQL query to execute.
        chunksize : int, optional
            The number of rows to fetch at a time, by default None.
        args : Any
            additional arguments.
        kwargs : Any
            The keyword arguments to format the SQL query with.

        Yields
        ------
        DataFrame
            A pandas DataFrame containing the query results.
        """
        with (
            self._engine.connect().execution_options(stream_results=True) as conn,
            conn.begin(),
        ):
            for df in pd.read_sql_query(query.format(**kwargs), conn, chunksize=chunksize):
                assert isinstance(df, pd.DataFrame)
                yield df.pipe(DataFrame)

    def get_df(
        self,
        query: str,
        *args: Any,
        **kwargs: Any,
    ) -> DataFrame:
        """
        A method that returns a pandas DataFrame from a SQL query.

        Parameters
        ----------
        query : str
            The SQL query to execute.
        args : Any
            additional arguments.
        kwargs : Any
            The keyword arguments to format the SQL query with.

        Returns
        -------
        DataFrame
            A pandas DataFrame containing the query results.
        """
        with (
            self._engine.connect().execution_options(stream_results=True) as conn,
            conn.begin(),
        ):
            df = pd.read_sql_query(query.format(**kwargs), conn, chunksize=None)
            assert isinstance(df, pd.DataFrame)
            return df.pipe(DataFrame)
