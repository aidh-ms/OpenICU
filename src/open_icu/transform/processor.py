import gc
from pathlib import Path

import polars as pl

from open_icu.config.dataset.source.config.field import ConstantFieldConfig
from open_icu.config.dataset.source.config.table import BaseTableConfig, TableConfig


def _process_table(table: BaseTableConfig, path: Path) -> pl.LazyFrame:
    lf = pl.scan_csv(
        path / table.path,
        schema_overrides=table.dtypes,
        infer_schema=False,
        low_memory=True,
    )
    lf = lf.select(table.dtypes.keys())

    for callback in table.pre_callbacks:
        lf = callback.call(lf)

    for field in table.fields:
        if isinstance(field, ConstantFieldConfig):
            lf = lf.with_columns(
                pl.lit(field.constant).cast(pl.String).alias(field.name)
            )

        if field.type == "datetime":
            lf = lf.with_columns(
                pl.col(field.name).str.to_datetime(**field.params).alias(field.name)
            )

    for callback in table.callbacks:
        lf = callback.call(lf)

    return lf


def process_table(table: TableConfig, path: Path, output_path: Path, src: str) -> None:
    lf = _process_table(table, path)

    post_callbacks = [*table.post_callbacks]
    for join_table in table.join:
        # Use broadcast join with small right table
        join_lf = _process_table(join_table, path)
        lf = lf.join(
            join_lf,
            how=join_table.how,  # type: ignore[arg-type]
            coalesce=True,  # Reduces memory by coalescing join keys
            **join_table.join_params  # type: ignore[arg-type]
        )
        post_callbacks.extend(join_table.post_callbacks)

    for callback in post_callbacks:
        lf = callback.call(lf)

    # Process each event
    codes_path = output_path / "metadata" / "codes.parquet"
    codes_path.parent.mkdir(parents=True, exist_ok=True)
    if codes_path.exists():
        codes_lf = pl.scan_parquet(codes_path)
    else:
        codes_lf = pl.LazyFrame(
            {"code": [], "description": [], "parent_codes": []},
            schema={"code": pl.String, "description": pl.String, "parent_codes": pl.String}
        )

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

        # Add code column and drop original code fields
        event_lf = event_lf.with_columns(code_expr)
        event_lf = event_lf.drop(event.fields.code)

        # Apply event callbacks
        for callback in event.callbacks:
            event_lf = callback.call(event_lf)

        # Reorder columns
        event_lf = event_lf.select([
            pl.col("subject_id").cast(pl.Int64),
            pl.col("time").cast(pl.Datetime(time_unit="us")),
            pl.col("code").cast(pl.String),
            pl.col("numeric_value").cast(pl.Float32),
            pl.col("text_value").cast(pl.String),
        ] + [pl.col(col).cast(pl.String) for col in event.fields.extension.keys()])

        # Ensure output directory exists
        output_data_path = output_path / "data" / src / table.name
        output_data_path.mkdir(parents=True, exist_ok=True)

        # Write to parquet with streaming
        output_file = output_data_path / f"{event.name}.parquet"
        event_lf.sink_parquet(
            output_file,
        )

        event_codes_lf = event_lf.select("code").unique()
        event_codes_lf = event_codes_lf.with_columns([
            pl.lit(None).alias("description").cast(pl.String),
            pl.lit(None).alias("parent_codes").cast(pl.String)
        ])

        del event_lf
        gc.collect()

        # Collect unique codes
        codes_lf = pl.concat([
            codes_lf,
            event_codes_lf,
        ]).unique(subset=["code"])

    temp_codes_path = output_path / "metadata" / "codes_temp.parquet"
    codes_lf.sink_parquet(
        temp_codes_path,
    )
    temp_codes_path.replace(codes_path)

    del lf
    del codes_lf
    gc.collect()
