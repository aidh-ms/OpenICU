from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from open_icu.config.source import TableConfig
from open_icu.meds.schema import OpenICUMEDSData


def process_table(table: TableConfig, path: Path, output_path: Path, src: str) -> None:
    table_paths = {
        join_table.name: join_table.path
        for join_table in table.join
    }
    table_paths[table.name] = table.path
    table_field_dtypes = table.table_field_dtypes

    tables: dict[str, dd.DataFrame] = {}
    for table_name, fields in table.table_field_dtypes.items():
        dtypes = {
            name: dtype if dtype != "datetime" else "string"
            for name, dtype in table_field_dtypes[table_name].items()
        }
        date_fields = [name for name, dtype in table_field_dtypes[table_name].items() if dtype == "datetime"]

        ddf = dd.read_csv(
            path / table_paths[table_name],
            usecols=fields.keys(),
            dtype=dtypes,
            parse_dates=date_fields,
            engine="pyarrow",
            dtype_backend="pyarrow",
        )

        for const_field, const_value in table.table_constants.get(table_name, {}).items():
            ddf[const_field] = const_value

        tables[table_name] = ddf

    for table_name, dt_fields in table.calc_datetime_fields.items():
        df = tables[table_name]
        for dt_field in dt_fields:
            df[dt_field.field] = dd.to_datetime(
                df[dt_field.year.field].astype("string").str.zfill(4) + "-" +
                df[dt_field.month.field].astype("string").str.zfill(2) + "-" +
                df[dt_field.day.field].astype("string").str.zfill(2) + " " +
                df[dt_field.time.field].astype("string"),
            ) + dd.to_timedelta(df[dt_field.offset.field].abs(), unit="m")
        tables[table_name] = df

    df = tables[table.name]
    for join in table.join:
        df = df.merge(
            tables[join.name],
            **join.join_params,
            how=join.how
        )

    for event in table.events:
        _df = df[event.field_names]
        if event.fields.text_value is None:
            _df["text_value"] = None
        if event.fields.numeric_value is None:
            _df["numeric_value"] = None

        _df = _df.dropna(subset=event.filters.dropna)
        _df = _df.rename(columns=event.column_mapping)

        codes = []
        def _map(df: pd.DataFrame) -> pd.DataFrame:
            codes_df = df[event.fields.code].drop_duplicates()
            codes_df["code"] = codes_df[event.fields.code[0]].str.cat(codes_df[event.fields.code[1:]], sep="//")
            codes.append(codes_df[["code"]])

            df = df.merge(
                codes_df,
                on=event.fields.code,
                how="left"
            ).drop(columns=event.fields.code)

            return df
        _df = _df.map_partitions(_map)

        _df.to_parquet(
            output_path / "data",
            name_function=lambda i: f"{src}_{table.name}_{event.name}_{i}.parquet",
            write_index=False,
            schema=OpenICUMEDSData.schema()
        )

        _codes_df = pd.concat(codes).drop_duplicates(subset=["code"])
        _medadata_df = dd.from_pandas(_codes_df, npartitions=1)
        _medadata_df["description"] = None
        _medadata_df["parent_codes"] = None

        codes_path = output_path / "metadata" / "codes.parquet"
        if codes_path.exists():
            existing_codes = dd.read_parquet(codes_path)
            _medadata_df = dd.concat([existing_codes, _medadata_df]).drop_duplicates(subset=["code"])

        _medadata_df.repartition(npartitions=1).to_parquet(
            codes_path.parent,
            name_function=lambda i: "codes.parquet",
            write_index=False,
        )
