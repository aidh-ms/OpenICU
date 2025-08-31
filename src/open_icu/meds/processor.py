from pathlib import Path

import dask.dataframe as dd

from open_icu.config.source import TableConfig


def process_table(table: TableConfig, path: Path, output_path: Path) -> None:
    table_paths = {
        join_table.name: join_table.path
        for join_table in table.join
    }
    table_paths[table.name] = table.path
    table_field_dtypes = table.table_field_dtypes()

    tables: dict[str, dd.DataFrame] = {}
    for table_name, fields in table.table_field_names().items():
        dtypes = {
            name: dtype if dtype != "datetime" else "string"
            for name, dtype in table_field_dtypes[table_name].items()
        }
        date_fields = [name for name, dtype in table_field_dtypes[table_name].items() if dtype == "datetime"]

        tables[table_name] = dd.read_csv(
            path / table_paths[table_name],
            usecols=fields,
            dtype=dtypes,
            parse_dates=date_fields
        )

    df = tables[table.name]
    for join in table.join:
        df = df.merge(
            tables[join.name],
            **join.join_params(),
            how=join.how
        )

    for event in table.events:

        _df = df[event.field_names()]
        if event.fields.text_value is None:
            _df["text_value"] = ""
        if event.fields.numeric_value is None:
            _df["numeric_value"] = 0.0

        _df = _df.rename(columns={
            event.fields.subject_id: "subject_id",
            event.fields.time: "time",
            event.fields.numeric_value: "numeric_value",
            event.fields.text_value: "text_value",
        })
        _df["code"] = df[event.fields.code].apply(lambda c: c.str.cat(sep="//"), axis=1)
        _df = _df.drop(columns=event.fields.code)
        _df.to_parquet(output_path / "data", name_function=lambda i: f"{table.name}_{event.name}_{i}.parquet")

        _medadata_df = _df[["code"]].drop_duplicates()
        _medadata_df["description"] = None
        _medadata_df["parent_codes"] = None
        _medadata_df.to_parquet(output_path / "metadata", name_function=lambda i: f"{table.name}_{event.name}_{i}.parquet")
