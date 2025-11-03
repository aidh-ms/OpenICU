from pathlib import Path

import polars as pl

from open_icu.config.dataset.source.config.field import ConstantFieldConfig
from open_icu.config.dataset.source.config.table import JsonTableConfig, TableConfig


def _process_join_table(join_table: JsonTableConfig, path: Path) -> pl.LazyFrame:
    lf = pl.scan_csv(
        path / join_table.path,
    )

    for field in join_table.fields:
        if isinstance(field, ConstantFieldConfig):
            lf = lf.with_columns(
                pl.lit(field.constant).alias(field.name)
            )

        if field.type == "datetime":
            lf = lf.with_columns(
                pl.col(field.name).str.to_datetime().alias(field.name)
            )

    for callback in join_table.callbacks:
        lf = callback.call(lf)

    return lf


def process_table(table: TableConfig, path: Path, output_path: Path, src: str) -> None:
    lf = pl.scan_csv(path / table.path)

    for field in table.fields:
        if isinstance(field, ConstantFieldConfig):
            lf = lf.with_columns(
                pl.lit(field.constant).alias(field.name)
            )

        if field.type == "datetime":
            lf = lf.with_columns(
                pl.col(field.name).str.to_datetime().alias(field.name)
            )

    for join_table in table.join:
        lf = lf.join(
            _process_join_table(join_table, path),
            how=join_table.how,  # type: ignore[arg-type]
            **join_table.join_params  # type: ignore[arg-type]
        )

    for callback in table.callbacks:
        lf = callback.call(lf)

    # Process each event
    all_codes = []

    for event in table.events:
        event_lf = lf

        # Add missing columns
        if event.fields.text_value is None:
            event_lf = event_lf.with_columns(pl.lit(None).alias("text_value"))
        if event.fields.numeric_value is None:
            event_lf = event_lf.with_columns(pl.lit(None).alias("numeric_value"))

        # Rename columns
        fields = event.fields.model_dump()
        extension = fields.pop("extension")
        mapping = {
            field: name
            for name, field in fields.items()
            if field is not None and not isinstance(field, list)
        } | {
            field: name
            for name, field in extension.items()
            if field is not None
        }
        event_lf = event_lf.rename(mapping)

        # Create code column by concatenating code fields
        if len(event.fields.code) > 1:
            code_expr = pl.concat_str(
                [pl.col(field) for field in event.fields.code],
                separator="//",
                ignore_nulls=True
            ).alias("code")
        else:
            code_expr = pl.col(event.fields.code[0]).fill_null("").alias("code")

        # Collect unique codes
        codes_df = (
            event_lf
            .select(event.fields.code)
            .unique()
            .with_columns(code_expr)
            .select("code")
            .collect()
        )
        all_codes.append(codes_df)

        # Add code column and drop original code fields
        event_lf = event_lf.with_columns(code_expr)
        event_lf = event_lf.drop(event.fields.code)

        # Apply event callbacks
        for callback in event.callbacks:
            event_lf = callback.call(event_lf)

        # Reorder columns
        ordering = event.fields.model_dump()
        event_lf = event_lf.select(list((ordering | ordering.pop("extension")).keys()))

        # Ensure output directory exists
        output_data_path = output_path / "data" / src / table.name
        output_data_path.mkdir(parents=True, exist_ok=True)

        # Write to parquet
        # Polars doesn't support custom name functions like Dask, so we write to a single file
        output_file = output_data_path / f"{event.name}.parquet"
        event_lf.sink_parquet(
            output_file,
            compression="snappy"
        )

    # Process metadata codes
    if all_codes:
        codes_df = pl.concat(all_codes).unique(subset=["code"])
        codes_df = codes_df.with_columns([
            pl.lit(None).alias("description"),
            pl.lit(None).alias("parent_codes")
        ])

        codes_path = output_path / "metadata" / "codes.parquet"
        codes_path.parent.mkdir(parents=True, exist_ok=True)

        if codes_path.exists():
            existing_codes = pl.read_parquet(codes_path)
            codes_df = pl.concat([existing_codes, codes_df]).unique(subset=["code"])

        codes_df.write_parquet(codes_path, compression="snappy")
