from pathlib import Path

import dask.dataframe as dd

from open_icu.config.source import TableConfig
from open_icu.meds.schema import OpenICUMEDSData


def process_table(table: TableConfig, path: Path, output_path: Path) -> None:
    table_paths = {
        join_table.name: join_table.path
        for join_table in table.join
    }
    table_paths[table.name] = table.path
    table_field_dtypes = table.table_field_dtypes

    tables: dict[str, dd.DataFrame] = {}
    for table_name, fields in table.table_field_names.items():
        dtypes = {
            name: dtype if dtype != "datetime" else "string"
            for name, dtype in table_field_dtypes[table_name].items()
        }
        date_fields = [name for name, dtype in table_field_dtypes[table_name].items() if dtype == "datetime"]

        ddf = dd.read_csv(
            path / table_paths[table_name],
            usecols=fields,
            dtype=dtypes,
            parse_dates=date_fields,
            engine="pyarrow",
            dtype_backend="pyarrow",
        )

        for const_field, const_value in table.table_constants.get(table_name, {}).items():
            ddf[const_field] = const_value

        tables[table_name] = ddf

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
            _df["text_value"] = ""
        if event.fields.numeric_value is None:
            _df["numeric_value"] = 0

        _df = _df.dropna(subset=event.filters.dropna)
        _df = _df.rename(columns=event.column_mapping)

        # def _apply(df: pd.DataFrame) -> pd.Series:
        #     return df[event.fields.code[0]].str.cat(df[event.fields.code[1:]], sep="//")

        codes = _df[event.fields.code].drop_duplicates().compute()
        # codes["code"] = codes[event.fields.code].apply(lambda c: c.str.cat(sep="//"), axis=1, meta=(None, "string"))
        codes["code"] = codes[event.fields.code[0]].str.cat(codes[event.fields.code[1:]], sep="//")
        # codes["code"] = codes.map_partitions(_apply, meta=(None, "string"))

        _df = _df.merge(
            codes,
            on=event.fields.code,
            how="left"
        )

        _df = _df.drop(columns=event.fields.code)

        _df.to_parquet(
            output_path / "data",
            name_function=lambda i: f"{table.name}_{event.name}_{i}.parquet",
            write_index=False,
            schema=OpenICUMEDSData.schema()
        )

        _medadata_df = codes[["code"]].copy()
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
