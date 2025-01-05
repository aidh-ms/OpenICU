from functools import lru_cache
from typing import Iterator

import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy import Connection, create_engine


class PandasDatabaseMixin:
    @lru_cache
    def _create_conn(self, connection_uri: str) -> Connection:
        engine = create_engine(connection_uri)
        return engine.connect().execution_options(stream_results=True)

    def iter_query_df(
        self,
        connection_uri: str,
        sql: str = "",
        chunksize: int | None = None,
        **kwargs: str,
    ) -> Iterator[DataFrame]:
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
        with (
            self._create_conn(connection_uri) as conn,
            conn.begin(),
        ):
            df = pd.read_sql_query(sql.format(**kwargs), conn, chunksize=None)
            assert isinstance(df, pd.DataFrame)
            return df.pipe(DataFrame)
