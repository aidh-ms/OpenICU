"""Extraction step implementation for converting ICU data to MEDS format.

This module implements the ExtractionStep class that orchestrates the extraction
of data from source CSV files, applies transformations via callbacks, performs
joins, and outputs MEDS-compliant Parquet files.
"""

import gc
from pathlib import Path

import polars as pl
from polars import LazyFrame

from open_icu.callbacks.interpreter import parse_expr
from open_icu.logging import get_logger
from open_icu.steps.base.step import ConfigurableBaseStep
from open_icu.steps.extraction.config.event import EventConfig
from open_icu.steps.extraction.config.step import ExtractionStepConfig
from open_icu.steps.extraction.config.table import BaseTableConfig, TableConfig, TableType
from open_icu.steps.extraction.registry import dataset_config_registry
from open_icu.storage.project import OpenICUProject

logger = get_logger(__name__)


class ExtractionStep(ConfigurableBaseStep[ExtractionStepConfig, TableConfig]):
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
        config = ExtractionStepConfig.load(config_path)
        return cls(project, config, dataset_config_registry)

    def extract(self) -> None:
        """Execute the data extraction workflow.

        For each table configuration in the registry:
        1. Read the source table
        2. Perform joins with related tables
        3. Apply post-join callbacks, filters, and transformations
        4. Extract events with column mappings
        5. Write MEDS-compliant Parquet files

        The extracted data is written to workspace_dir/dataset/table/event.parquet
        """
        for cfg in self._config.config.data:
            for table in self._registry.filter(
                cfg.name,
                cfg.version,
                includes=cfg.includes,
                excludes=cfg.excludes,

            ):
                logger.info(
                    "Extracting table %s from dataset %s (version %s)",
                    table.name,
                    cfg.name,
                    cfg.version,
                )
                self._extract(table, cfg.path)

    def _extract(self, table: TableConfig, path: Path) -> None:
        try:
            lf = self._read_table(table, path)

            for join_table in table.join:
                # Use broadcast join with small right table
                logger.debug(
                    "Joining table %s with %s",
                    table.name,
                    join_table.path,
                )
                join_lf = self._read_table(join_table, path)

                lf = lf.join(
                    join_lf,
                    how=join_table.how,  # ty: ignore[invalid-argument-type]
                    coalesce=True,  # Reduces memory by coalescing join keys
                    **join_table.join_params,  # ty: ignore[invalid-argument-type]
                )

                lf = self._apply_callbacks(
                    lf,
                    join_table.post_join_callbacks,
                    callback_type="Post-join callback",
                )
                lf = self._apply_filters(
                    lf,
                    join_table.post_join_filters,
                    callback_type="Post-join filter",
                )

        except FileNotFoundError as e:
            logger.warning("Skipping table %s: %s", table.name, e)
            return

        logger.info("Processing table %s", table.name)

        lf = self._apply_callbacks(
            lf,
            table.post_join_callbacks,
            callback_type="Table post-join callback",
        )
        lf = self._apply_filters(
            lf,
            table.post_join_filters,
            callback_type="Table post-join filter",
        )
        lf = self._apply_transformations(
            lf,
            table.transformations,
            callback_type="Table transformation",
        )

        for event in table.events:
            logger.debug(
                "Processing event %s for table %s",
                event.name,
                table.name,
            )

            event_identifier: tuple[str, ...] = table.identifier_tuple[1:] + (event.name,)
            event_lf = lf

            event_lf = self._apply_callbacks(
                event_lf,
                event.pre_callbacks,
                callback_type="Event pre-callback",
            )

            # Add missing columns
            if event.columns.text_value is None:
                event_lf = event_lf.with_columns(
                    pl.lit(None, dtype=pl.String).alias("text_value")
                )
            if event.columns.numeric_value is None:
                event_lf = event_lf.with_columns(
                    pl.lit(None, dtype=pl.Float32).alias("numeric_value")
                )

            # Rename columns
            columns = event.columns.model_dump()
            extension = columns.pop("extension")
            columns.pop("code", None)

            for col_name, col_expr in columns.items():
                if col_expr is not None:
                    event_lf = event_lf.with_columns(
                        self._parse_expr(
                            event_lf,
                            col_expr,
                            callback_type="Event column mapping",
                        ).alias(col_name)
                    )

            for col_name, col_expr in extension.items():
                if col_expr is not None:
                    event_lf = event_lf.with_columns(
                        self._parse_expr(
                            event_lf,
                            col_expr,
                            callback_type="Event extension mapping",
                        ).alias(col_name)
                    )

            # Create code column.
            #
            # Final code structure:
            # db_name // table_name // code_prefix // columns.code // code_suffix
            #
            # db_name and table_name are automatic. code_prefix, columns.code,
            # and code_suffix are configured. columns.code contains optional
            # user-defined code parts such as unit, route, specimen, or method.
            code_expr = self._build_code_expr(event_lf, table, event)

            # Add constructed MEDS code column
            event_lf = event_lf.with_columns(code_expr)

            # Apply event callbacks
            event_lf = self._apply_callbacks(
                event_lf,
                event.callbacks,
                callback_type="Event callback",
            )

            event_lf = self._apply_filters(
                event_lf,
                event.filters,
                callback_type="Event filter",
            )

            event_lf = self._apply_transformations(
                event_lf,
                event.transformations,
                callback_type="Event transformation",
            )

            # Reorder columns
            event_lf = event_lf.select([
                pl.col("subject_id").cast(pl.Int64),
                pl.col("time").cast(pl.Datetime(time_unit="us")),
                pl.col("code").cast(pl.String),
                pl.col("numeric_value").cast(pl.Float32, strict=False),
                pl.col("text_value").cast(pl.String),
            ] + [pl.col(col) for col in event.columns.extension.keys()])

            event_lf = self._apply_filters(
                event_lf,
                event.output_filters,
                callback_type="Event output filter",
            )

            # Ensure output directory exists
            assert self._workspace_dir is not None
            output_data_path = Path(self._workspace_dir.path, *event_identifier[:-1])
            output_data_path.mkdir(parents=True, exist_ok=True)

            # Write to parquet with streaming
            output_file = output_data_path / f"{event.name}.parquet"
            logger.info(
                "Writing event %s for table %s to %s",
                event.name,
                table.name,
                output_file,
            )

            if output_file.exists():
                logger.info(
                    "Existing output found for event %s, appending to it",
                    event.name,
                )

                existing_lf = pl.scan_parquet(output_file)

                event_lf = pl.concat(
                    [existing_lf, event_lf],
                    how="vertical",
                )
                tmp_output_file = output_data_path / f"{event.name}.tmp.parquet"

                event_lf.sink_parquet(tmp_output_file)
                tmp_output_file.replace(output_file)
            else:
                event_lf.sink_parquet(output_file)

            del event_lf

        del lf
        gc.collect()

    @staticmethod
    def _resolve_source(table: BaseTableConfig, path: Path) -> Path | list[Path]:
        """Resolve a table path to a concrete source for Polars scanners.

        A plain path must point at a single existing file. A path containing a
        glob character (``*``, ``?`` or ``[``) is expanded relative to the
        dataset root, which supports datasets distributed as many partitioned
        files (e.g. HiRID's ``observation_tables/parquet/part-*.parquet``).
        Both ``scan_parquet`` and ``scan_csv`` accept the resulting list.
        """
        if any(char in table.path for char in "*?["):
            matches = sorted(path.glob(table.path))
            if not matches:
                raise FileNotFoundError(f"no files match ({path / table.path})")
            return matches

        file_path = path / table.path
        if not file_path.exists():
            raise FileNotFoundError(f"file not found ({file_path})")
        return file_path

    def _read_table(self, table: BaseTableConfig, path: Path) -> pl.LazyFrame:
        """Read and transform a table from CSV.

        Scans the CSV file, applies schema overrides, executes pre-callbacks,
        applies pre-filters, converts datetime columns, executes callbacks, and
        applies filters.

        Args:
            table: Configuration for the table to read
            path: Base path to the data directory

        Returns:
            LazyFrame with the transformed table data
        """
        source = self._resolve_source(table, path)

        if table.type == TableType.PARQUET:
            lf = pl.scan_parquet(source)
            lf = lf.select(table.dtypes.keys())
            # Parquet carries its own schema, so cast the non-temporal columns to
            # the declared dtypes. Datetime columns ("datetime" maps to String)
            # are handled below to support both native timestamps and strings.
            casts = [
                pl.col(col.name).cast(col.dtype, strict=False)
                for col in table.columns
                if col.type != "datetime"
            ]
            if casts:
                lf = lf.with_columns(casts)
        else:
            lf = pl.scan_csv(
                source,
                schema_overrides=table.dtypes,
                infer_schema=False,
                low_memory=True,
            )
            lf = lf.select(table.dtypes.keys())

        lf = self._apply_callbacks(
            lf,
            table.pre_callbacks,
            callback_type="Table pre-callback",
        )
        lf = self._apply_filters(
            lf,
            table.pre_filters,
            callback_type="Table pre-filter",
        )

        datetime_cols = [col for col in table.columns if col.type == "datetime"]
        if datetime_cols:
            # CSV reads datetimes as strings; Parquet may store them either as
            # native temporal types or as strings, so branch on the actual dtype.
            schema = lf.collect_schema()
            for col in datetime_cols:
                if schema.get(col.name) == pl.String:
                    lf = lf.with_columns(
                        pl.col(col.name).str.to_datetime(**col.params).alias(col.name)
                    )
                else:
                    lf = lf.with_columns(
                        pl.col(col.name).cast(pl.Datetime("us"), strict=False).alias(col.name)
                    )

        lf = self._apply_callbacks(
            lf,
            table.callbacks,
            callback_type="Table callback",
        )
        lf = self._apply_filters(
            lf,
            table.filters,
            callback_type="Table filter",
        )

        return lf

    def _build_code_expr(
        self,
        lf: LazyFrame,
        table: TableConfig,
        event: EventConfig,
    ) -> pl.Expr:
        """Build the MEDS code expression for an event.

        The final code is built as:

            db_name // table_name // code_prefix // columns.code // code_suffix

        The db_name and table_name parts are automatic. The configured code
        prefix is inserted after db/table. Additional user-defined code parts,
        such as event names, units, routes, specimens, or methods, are provided
        through columns.code. The configured code suffix is appended last.
        """
        code_parts: list[pl.Expr] = []

        code_parts.extend(
            self._parse_expr(
                lf,
                expr,
                callback_type="Event code prefix",
            )
            for expr in event.code_prefix
        )

        code_parts.extend(
            self._parse_expr(
                lf,
                expr,
                callback_type="Event code part",
            )
            for expr in event.columns.code
        )

        code_parts.extend(
            self._parse_expr(
                lf,
                expr,
                callback_type="Event code suffix",
            )
            for expr in event.code_suffix
        )
        if len(code_parts) == 1:
            return code_parts[0].cast(pl.String).alias("code")

        return pl.concat_str(
            [expr.cast(pl.String) for expr in code_parts],
            separator="//",
            ignore_nulls=True,
        ).alias("code")

    @staticmethod
    def _parse_expr(lf: LazyFrame, expr: str, callback_type: str) -> pl.Expr:
        """Parse a configured expression and validate that it returns a Polars expression."""
        result = parse_expr(lf, expr)

        if not isinstance(result, pl.Expr):
            raise TypeError(
                f"{callback_type} {expr!r} must return a Polars Expr, "
                f"got {type(result).__name__}"
            )

        return result

    def _apply_callbacks(
        self,
        lf: LazyFrame,
        callbacks: list[str],
        callback_type: str,
    ) -> LazyFrame:
        """Apply expression callbacks with LazyFrame.with_columns."""
        for expr in callbacks:
            lf = lf.with_columns(
                self._parse_expr(
                    lf,
                    expr,
                    callback_type=callback_type,
                )
            )

        return lf

    def _apply_filters(
        self,
        lf: LazyFrame,
        filters: list[str],
        callback_type: str,
    ) -> LazyFrame:
        """Apply expression filters with LazyFrame.filter."""
        for expr in filters:
            lf = lf.filter(
                self._parse_expr(
                    lf,
                    expr,
                    callback_type=callback_type,
                )
            )

        return lf

    @staticmethod
    def _apply_transformations(
        lf: LazyFrame,
        transformations: list[str],
        callback_type: str,
    ) -> LazyFrame:
        """Apply frame transformations that return LazyFrame objects."""
        for expr in transformations:
            result = parse_expr(lf, expr)

            if not isinstance(result, LazyFrame):
                raise TypeError(
                    f"{callback_type} {expr!r} must return a LazyFrame, "
                    f"got {type(result).__name__}"
                )

            lf = result

        return lf
