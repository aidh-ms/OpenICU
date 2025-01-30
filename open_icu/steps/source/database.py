from functools import lru_cache
from typing import Iterator

import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy import Connection, create_engine


class PandasDatabaseMixin:
    """
    A mixin class for querying databases and returning pandas DataFrames.
    """

    @lru_cache
    def _create_conn(self, connection_uri: str) -> Connection:
        """
        create a connection to the database.
        """
        engine = create_engine(connection_uri)
        return engine.connect().execution_options(stream_results=True)

    def iter_query_df(
        self,
        connection_uri: str,
        sql: str = "",
        chunksize: int | None = None,
        **kwargs: str,
    ) -> Iterator[DataFrame]:
        """
        A generator function that yields pandas DataFrames from a SQL query.

        Parameters
        ----------
        connection_uri : str
            The connection URI for the database.
        sql : str, optional
            The SQL query to execute, by default "".
        chunksize : int, optional
            The number of rows to fetch at a time, by default None.
        kwargs : str
            The keyword arguments to format the SQL query with.

        Yields
        ------
        DataFrame
            A pandas DataFrame containing the query results.
        """
        with (
            self._create_conn(connection_uri) as conn,
            conn.begin(),
        ):
            for df in pd.read_sql_query(sql.format(**kwargs), conn, chunksize=chunksize):
                assert isinstance(df, pd.DataFrame)
                yield df.pipe(DataFrame)

    def get_query_df(
        self,
        connection_uri: str,
        sql: str = "",
        **kwargs: str,
    ) -> DataFrame:
        """
        A method that returns a pandas DataFrame from a SQL query.

        Parameters
        ----------
        connection_uri : str
            The connection URI for the database.
        sql : str, optional
            The SQL query to execute, by default "".
        kwargs : str
            The keyword arguments to format the SQL query with.

        Returns
        -------
        DataFrame
            A pandas DataFrame containing the query results.
        """
        with (
            self._create_conn(connection_uri) as conn,
            conn.begin(),
        ):
            df = pd.read_sql_query(sql.format(**kwargs), conn, chunksize=None)
            assert isinstance(df, pd.DataFrame)
            return df.pipe(DataFrame)
