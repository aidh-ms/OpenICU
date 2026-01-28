"""Extraction step implementation for converting ICU data to MEDS format.

This module implements the ExtractionStep class that orchestrates the extraction
of data from source CSV files, applies transformations via callbacks, performs
joins, and outputs MEDS-compliant Parquet files.
"""
import gc
from pathlib import Path
from uuid import uuid4

import polars as pl

from open_icu.callbacks.interpreter import parse_expr
from open_icu.logging import get_logger
from open_icu.steps.base.step import ConfigurableBaseStep
from open_icu.steps.concept.config.concept import ConceptConfig, MappingConfig
from open_icu.steps.concept.config.step import ConceptStepConfig
from open_icu.steps.concept.registry import concept_config_registry
from open_icu.storage.project import OpenICUProject

logger = get_logger(__name__)


class ConceptStep(ConfigurableBaseStep[ConceptStepConfig, ConceptConfig]):
    """Data extraction step for transforming source ICU data to MEDS format.

    Reads CSV files specified in TableConfig objects, applies pre/post callbacks,
    performs table joins, extracts events with field mappings, and writes
    MEDS-compliant Parquet files to the workspace directory.
    """
    @classmethod
    def load(cls, project: OpenICUProject, config_path: Path) -> "ConceptStep":
        """Load an extraction step from a configuration file.

        Args:
            project: The OpenICU project to operate within
            config_path: Path to the extraction configuration YAML file

        Returns:
            An initialized ExtractionStep instance
        """
        config = ConceptStepConfig.load(config_path)
        return cls(project, config, concept_config_registry)

    def extract(self) -> None:
        extraction_dataset = self._project.datasets.get(self._config.config.extraction_step.lower())
        if not extraction_dataset:
            logger.warning("skipping concept step: extraction dataset not found")
            return

        codes_path = extraction_dataset.metadata_path / "codes.parquet"
        if not codes_path.exists():
            logger.warning("skipping concept step: extraction codes.parquet not found")
            return
        codes_df = pl.read_parquet(extraction_dataset.metadata_path / "codes.parquet")

        for concept in self._registry.values():
            logger.info("extracting concept: %s", concept.name)
            for mapping in concept.mappings:
                self.extract_mapping(
                    concept,
                    mapping,
                    codes_df,
                    extraction_dataset.data_path
                )

    def extract_mapping(
        self,
        concept: ConceptConfig,
        mapping: MappingConfig,
        codes_df: pl.DataFrame,
        dataset_path: Path,
    ) -> None:
        mapping_codes = codes_df.filter(pl.col("code").str.contains(mapping.regex))["code"]

        for dataset, version, table, event in mapping_codes.str.split("//").list.head(4).unique().to_list():
            data_path = dataset_path / dataset / version / table / f"{event}.parquet"
            if not data_path.exists():
                logger.warning(
                    "skipping mapping for concept %s: file not found (%s)",
                    concept.name,
                    data_path
                )
                continue

            lf = pl.scan_parquet(data_path).filter(pl.col("code").is_in(mapping_codes))

            # extension columns
            lf = lf.with_columns(pl.lit(dataset).alias("dataset"))
            lf = lf.with_columns(pl.lit(version).alias("version"))
            lf = lf.with_columns(pl.lit(table).alias("table"))
            lf = lf.with_columns(pl.lit(event).alias("event"))
            for col_name, col_expr in concept.extension_columns.items():
                lf = lf.with_columns(parse_expr(lf, col_expr).alias(col_name))

            # value columns
            if mapping.columns.text_value is None:
                lf = lf.with_columns(pl.lit(None).alias("text_value"))
            else:
                lf = lf.with_columns(parse_expr(lf, mapping.columns.text_value).alias("text_value"))

            if mapping.columns.numeric_value is None:
                lf = lf.with_columns(pl.lit(None).alias("numeric_value"))
            else:
                lf = lf.with_columns(parse_expr(lf, mapping.columns.numeric_value).alias("numeric_value"))

            # code column
            lf = lf.with_columns(pl.lit(concept.code).alias("code"))

            for expr in mapping.filters:
                lf = lf.filter(parse_expr(lf, expr))
            lf = lf.select([
                pl.col("subject_id").cast(pl.Int64),
                pl.col("time").cast(pl.Datetime(time_unit="us")),
                pl.col("code").cast(pl.String),
                pl.col("numeric_value").cast(pl.Float32),
                pl.col("text_value").cast(pl.String),
            ] + [pl.col(col).cast(pl.String) for col in concept.extension_columns.keys()])

            assert self._workspace_dir is not None
            output_data_path = Path(self._workspace_dir.path, *concept.identifier_tuple[1:])
            output_data_path.mkdir(parents=True, exist_ok=True)

            output_file = output_data_path / f"{str(uuid4())}.parquet"
            lf.sink_parquet(
                output_file,
            )

            del lf
            gc.collect()
