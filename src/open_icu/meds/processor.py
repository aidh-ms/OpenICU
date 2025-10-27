from pathlib import Path

import polars as pl

from open_icu.config.source import TableConfig


def process_table(table: TableConfig, path: Path, output_path: Path, src: str) -> None:
    table_paths = {
        join_table.name: join_table.path
        for join_table in table.join
    }
    table_paths[table.name] = table.path
    table_field_dtypes = table.table_field_dtypes

    tables: dict[str, pl.LazyFrame] = {}
    for table_name, fields in table.table_field_dtypes.items():
        # Map config dtypes to Polars dtypes
        dtype_mapping = {
            "int64": pl.Int64,
            "int32": pl.Int32,
            "float64": pl.Float64,
            "float32": pl.Float32,
            "string": pl.Utf8,
            "datetime": pl.Utf8,  # Read as string first, then parse
        }

        dtypes = {
            name: dtype_mapping.get(dtype, pl.Utf8)
            for name, dtype in table_field_dtypes[table_name].items()
        }
        date_fields = [name for name, dtype in table_field_dtypes[table_name].items() if dtype == "datetime"]

        # Read CSV lazily with Polars
        lf = pl.scan_csv(  # type: ignore[call-arg]
            path / table_paths[table_name],
            dtypes=dtypes,
        )

        # Parse datetime columns
        for date_field in date_fields:
            lf = lf.with_columns(
                pl.col(date_field).str.to_datetime().alias(date_field)
            )

        # Add constant fields
        for const_field, const_value in table.table_constants.get(table_name, {}).items():
            lf = lf.with_columns(
                pl.lit(const_value).alias(const_field)
            )

        tables[table_name] = lf

    # Process calculated datetime fields
    for table_name, dt_fields in table.calc_datetime_fields.items():
        lf = tables[table_name]
        for dt_field in dt_fields:
            # Build datetime string from components
            datetime_expr = (
                pl.col(dt_field.year.field).cast(pl.Utf8).str.zfill(4) + pl.lit("-") +
                pl.col(dt_field.month.field).cast(pl.Utf8).str.zfill(2) + pl.lit("-") +
                pl.col(dt_field.day.field).cast(pl.Utf8).str.zfill(2) + pl.lit(" ") +
                pl.col(dt_field.time.field).cast(pl.Utf8)
            ).str.to_datetime()

            # Add offset in minutes
            offset_expr = pl.duration(minutes=pl.col(dt_field.offset.field).abs())

            lf = lf.with_columns(
                (datetime_expr + offset_expr).alias(dt_field.field)
            )
        tables[table_name] = lf

    # Start with main table
    lf = tables[table.name]

    # Perform joins
    for join in table.join:
        lf = lf.join(
            tables[join.name],
            how=join.how,  # type: ignore[arg-type]
            **join.join_params  # type: ignore[arg-type]
        )

    # Process offset datetime fields
    for table_name, odt_fields in table.offset_datetime_fields.items():
        for odt_field in odt_fields:
            offset_expr = pl.duration(minutes=pl.col(odt_field.offset.field).abs())
            lf = lf.with_columns(
                (pl.col(odt_field.base.field) + offset_expr).alias(odt_field.field)
            )

    # Process each event
    all_codes = []

    for event in table.events:
        # Select event fields
        event_lf = lf.select(event.field_names)

        # Add missing columns
        if event.fields.text_value is None:
            event_lf = event_lf.with_columns(pl.lit(None).alias("text_value"))
        if event.fields.numeric_value is None:
            event_lf = event_lf.with_columns(pl.lit(None).alias("numeric_value"))

        # Drop rows with missing values in specified columns
        if event.filters.dropna:
            event_lf = event_lf.drop_nulls(subset=event.filters.dropna)

        # Rename columns
        rename_mapping = event.column_mapping
        event_lf = event_lf.rename(rename_mapping)

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

        # Reorder columns
        event_lf = event_lf.select(event.column_order)

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
