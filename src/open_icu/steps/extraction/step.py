"""Extraction step implementation for converting ICU data to MEDS format.

This module implements the ExtractionStep class that orchestrates the extraction
of data from source CSV files, applies transformations via callbacks, performs
joins, and outputs MEDS-compliant Parquet files.
"""

import gc
from pathlib import Path

import polars as pl

from open_icu.callbacks.interpreter import parse_expr
from open_icu.logging import get_logger
from open_icu.steps.base.step import ConfigurableBaseStep
from open_icu.steps.extraction.config.column import ConstantColumnConfig
from open_icu.steps.extraction.config.step import ExtractionConfig
from open_icu.steps.extraction.config.table import BaseTableConfig, TableConfig
from open_icu.steps.extraction.registry import dataset_config_registery
from open_icu.storage.project import OpenICUProject

logger = get_logger(__name__)


class ExtractionStep(ConfigurableBaseStep[ExtractionConfig, TableConfig]):
    """Data extraction step for transforming source ICU data to MEDS format.

    Reads CSV files specified in TableConfig objects, applies pre/post callbacks,
    performs table joins, extracts events with column mappings, and writes
    MEDS-compliant Parquet files to the workspace directory.
    """
    @classmethod
    def load(cls, project: OpenICUProject, config_path: Path) -> "ExtractionStep":
        """Load an extraction step from a configuration file.

        Args:
            project: The OpenICU project to operate within
            config_path: Path to the extraction configuration YAML file

        Returns:
            An initialized ExtractionStep instance
        """
        config = ExtractionConfig.load(config_path)
        return cls(project, config, dataset_config_registery)

    def _read_table(self, table: BaseTableConfig, path) -> pl.LazyFrame:
        """Read and transform a table from CSV.

        Scans the CSV file, applies schema overrides, executes pre-callbacks,
        adds constant columns, converts datetime columns, and executes callbacks.

        Args:
            table: Configuration for the table to read
            path: Base path to the data directory

        Returns:
            LazyFrame with the transformed table data
        """
        file_path = path / table.path
        if not file_path.exists():
            raise FileNotFoundError(f"file not found ({file_path})")

        lf = pl.scan_csv(
            file_path,
            schema_overrides=table.dtypes,
            infer_schema=False,
            low_memory=True,
        )
        lf = lf.select(table.dtypes.keys())

        for expr in table.pre_callbacks:
            lf = lf.with_columns(parse_expr(lf, expr))

        for col in table.columns:
            if isinstance(col, ConstantColumnConfig):
                lf = lf.with_columns(
                    pl.lit(col.constant).cast(col.dtype).alias(col.name)
                )

            if col.type == "datetime":
                lf = lf.with_columns(
                    pl.col(col.name).str.to_datetime(**col.params).alias(col.name)
                )

        for expr in table.callbacks:
            lf = lf.with_columns(parse_expr(lf, expr))

        return lf

    def extract(self) -> None:
        """Execute the data extraction workflow.

        For each table configuration in the registry:
        1. Read the source table
        2. Perform joins with related tables
        3. Apply post-processing callbacks
        4. Extract events with column mappings
        5. Write MEDS-compliant Parquet files

        The extracted data is written to workspace_dir/dataset/table/event.parquet
        """
        paths = {
            cfg.name: cfg.path
            for cfg in self._config.config.data
        }
        for table in self._registry.values():
            path = paths.get(table.dataset)
            if path is None:
                logger.warning("skipping table %s: dataset path not found (%s)", table.name, path)
                continue

            try:
                lf = self._read_table(table, path)

                post_callbacks = [*table.post_callbacks]
                for join_table in table.join:
                    # Use broadcast join with small right table
                    join_lf = self._read_table(join_table, path)
                    lf = lf.join(
                        join_lf,
                        how=join_table.how,  # type: ignore[arg-type]
                        coalesce=True,  # Reduces memory by coalescing join keys
                        **join_table.join_params  # type: ignore[arg-type]
                    )
                    post_callbacks.extend(join_table.post_callbacks)
            except FileNotFoundError as e:
                logger.warning("skipping table %s: %s", table.name, e)
                continue

            logger.info("processing table %s", table.name)
            for expr in post_callbacks:
                lf = lf.with_columns(parse_expr(lf, expr))

            for event in table.events:
                event_lf = lf

                # Add missing columns
                if event.columns.text_value is None:
                    event_lf = event_lf.with_columns(pl.lit(None).alias("text_value"))
                if event.columns.numeric_value is None:
                    event_lf = event_lf.with_columns(pl.lit(None).alias("numeric_value"))

                # Rename columns
                columns = event.columns.model_dump()
                extension = columns.pop("extension")
                mapping = {
                    col_expr: col_name
                    for col_name, col_expr in columns.items()
                    if col_expr is not None and not isinstance(col_expr, list)
                } | {
                    col_expr: col_name
                    for col_name, col_expr in extension.items()
                    if col_expr is not None
                }
                for col_expr, col_name in mapping.items():
                    event_lf = event_lf.with_columns(parse_expr(event_lf, col_expr).alias(col_name))

                # Create code column by concatenating code columns
                if len(event.columns.code) > 1:
                    code_expr = pl.concat_str(
                        [parse_expr(event_lf, col_expr) for col_expr in event.columns.code],
                        separator="//",
                        ignore_nulls=True
                    ).alias("code")
                else:
                    code_expr = parse_expr(event_lf, event.columns.code[0]).fill_null("").alias("code")

                # Add code column and drop original code columns
                event_lf = event_lf.with_columns(code_expr)

                # Apply event callbacks
                for expr in event.callbacks:
                    event_lf = event_lf.with_columns(parse_expr(event_lf, expr))

                for expr in event.filters:
                    event_lf = event_lf.filter(parse_expr(event_lf, expr))

                # Reorder columns
                event_lf = event_lf.select([
                    pl.col("subject_id").cast(pl.Int64),
                    pl.col("time").cast(pl.Datetime(time_unit="us")),
                    pl.col("code").cast(pl.String),
                    pl.col("numeric_value").cast(pl.Float32),
                    pl.col("text_value").cast(pl.String),
                ] + [pl.col(col).cast(pl.String) for col in event.columns.extension.keys()])

                # Ensure output directory exists
                assert self._workspace_dir is not None
                output_data_path = self._workspace_dir.path / table.dataset / table.name
                output_data_path.mkdir(parents=True, exist_ok=True)

                # Write to parquet with streaming
                output_file = output_data_path / f"{event.name}.parquet"
                event_lf.sink_parquet(
                    output_file,
                )
                del event_lf

            del lf
            gc.collect()
