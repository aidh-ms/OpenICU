"""Concept step implementation for converting ICU data to MEDS format.

This module implements the ConceptStep class that orchestrates the extraction
of concept events from extracted MEDS event files, applies mappings based on
code patterns, and outputs MEDS-compliant Parquet files.
"""

import gc
from functools import cached_property
from graphlib import TopologicalSorter
from pathlib import Path
from uuid import uuid4

import polars as pl

from open_icu.callbacks.interpreter import parse_expr
from open_icu.logging import get_logger
from open_icu.steps.base.step import ConfigurableBaseStep
from open_icu.steps.concept.config.concept import (
    ComplexDatasetConceptConfig,
    ConceptConfig,
    DerivedDatasetConceptConfig,
    SimpleDatasetConceptConfig,
)
from open_icu.steps.concept.config.derived import BaseConceptTable
from open_icu.steps.concept.config.step import ConceptStepConfig
from open_icu.steps.concept.registry import concept_config_registry
from open_icu.storage.project import OpenICUProject

logger = get_logger(__name__)


class ConceptStep(ConfigurableBaseStep[ConceptStepConfig, ConceptConfig]):
    """Concept step for extracting MEDS concept events from ICU data.

    Reads extracted MEDS data specified in ConceptConfig objects, applies
    mappings based on code patterns, and writes MEDS-compliant Parquet files
    to the workspace directory.
    """

    @classmethod
    def load(cls, project: OpenICUProject, config_path: Path) -> "ConceptStep":
        """Load a concept step from a configuration file.

        Args:
            project: The OpenICU project to operate within
            config_path: Path to the concept configuration YAML file

        Returns:
            An initialized ConceptStep instance
        """
        config = ConceptStepConfig.load(config_path)
        return cls(project, config, concept_config_registry)

    def extract(self) -> None:
        datasets = {
            (dataset_config.name, dataset_config.version)
            for dataset_config in self._config.config.mapping_configs
        }

        for dataset, version in datasets:
            logger.info("Processing concepts for dataset %s (version %s)", dataset, version)
            depend_concepts = dict()

            for concept in self._registry.values():
                dataset_concept = concept.get_dataset_concept(dataset, version)
                if dataset_concept is None:
                    logger.debug(
                        "skipping concept %s for dataset %s (version %s): no dataset-specific config found",
                        concept.name,
                        dataset,
                        version,
                    )
                    continue

                if isinstance(dataset_concept, SimpleDatasetConceptConfig):
                    logger.debug(
                        "Extracting simple concept %s for dataset %s",
                        concept.identifier,
                        dataset,
                    )
                    self.extract_simple_concept(
                        concept,
                        dataset_concept,
                    )

                if isinstance(dataset_concept, (DerivedDatasetConceptConfig, ComplexDatasetConceptConfig)):
                    logger.debug(
                        "Registering dependent concept %s for dataset %s",
                        concept.identifier,
                        dataset,
                    )
                    depend_concepts[concept.identifier] = dataset_concept.dependencies

            for concept_id in TopologicalSorter(depend_concepts).static_order():
                logger.debug(
                    "Processing dependent concept %s for dataset %s",
                    concept_id,
                    dataset,
                )
                concept = self._registry.get(concept_id)
                assert concept is not None

                dataset_concept = concept.get_dataset_concept(dataset, version)
                if dataset_concept is None:
                    logger.debug(
                        "skipping concept %s for dataset %s (version %s): no dataset-specific config found",
                        concept.name,
                        dataset,
                        version,
                    )
                    continue

                if isinstance(dataset_concept, DerivedDatasetConceptConfig):
                    logger.debug(
                        "Extracting derived concept %s for dataset %s",
                        concept.identifier,
                        dataset,
                    )
                    self.extract_derived_concept(
                        concept,
                        dataset_concept,
                    )

                if isinstance(dataset_concept, ComplexDatasetConceptConfig):
                    logger.debug(
                        "Running complex concept %s for dataset %s",
                        concept.identifier,
                        dataset,
                    )
                    dataset_concept.fn(self._project)

    @property
    def extraction_dataset(self):
        extraction_dataset = self._project.datasets.get(self._config.config.extraction_step.lower())
        if not extraction_dataset:
            logger.warning("skipping concept step: extraction dataset not found")
            return
        return self._project.datasets.get(self._config.config.extraction_step.lower())

    @cached_property
    def codes_df(self) -> pl.DataFrame:
        extraction_dataset = self.extraction_dataset
        if not extraction_dataset:
            logger.warning("skipping concept step: extraction dataset not found")
            return pl.DataFrame()
        codes_path = extraction_dataset.metadata_path / "codes.parquet"
        if not codes_path.exists():
            logger.warning("skipping concept step: extraction codes.parquet not found")
            return pl.DataFrame()
        return pl.read_parquet(codes_path)

    def apply_limits(self, concept: ConceptConfig, lf: pl.LazyFrame) -> pl.LazyFrame:
        if concept.limits.min is not None:
            lf = lf.with_columns(
                pl.when(pl.col("numeric_value") < concept.limits.min)
                .then(None)
                .otherwise(pl.col("numeric_value"))
                .alias("numeric_value")
            )

        if concept.limits.max is not None:
            lf = lf.with_columns(
                pl.when(pl.col("numeric_value") > concept.limits.max)
                .then(None)
                .otherwise(pl.col("numeric_value"))
                .alias("numeric_value")
            )

        return lf

    def extract_simple_concept(
        self,
        concept: ConceptConfig,
        dataset_concept: SimpleDatasetConceptConfig,
    ) -> None:
        logger.debug(
            "Extracting simple concept %s for dataset %s",
            concept.identifier,
            dataset_concept.dataset,
        )
        assert self._workspace_dir is not None
        output_data_path = Path(self._workspace_dir.path, *concept.identifier_tuple[1:])
        output_dataset_path = output_data_path / dataset_concept.dataset
        output_dataset_path.mkdir(parents=True, exist_ok=True)

        for mapping in dataset_concept.mappings:
            dataset = dataset_concept.dataset
            version = dataset_concept.version
            table = mapping.pattern.table
            event = mapping.pattern.event

            table_path = self.extraction_dataset.data_path / dataset / version / table

            if event is None:
                data_paths = sorted(table_path.glob("*.parquet"))
            else:
                data_paths = [table_path / f"{event}.parquet"]

            if not data_paths:
                logger.warning(
                    "skipping mapping for concept %s: no event files found in %s",
                    concept.name,
                    table_path,
                )
                continue

            for data_path in data_paths:
                if not data_path.exists():
                    logger.warning(
                        "skipping mapping for concept %s: file not found (%s)",
                        concept.name,
                        data_path,
                    )
                    continue

                event_name = data_path.stem

                logger.debug(
                    "Loading source event %s/%s/%s/%s for concept %s",
                    dataset,
                    version,
                    table,
                    event_name,
                    concept.identifier,
                )

                lf = pl.scan_parquet(data_path).filter(
                    pl.col("code").str.contains(mapping.pattern.code)
                )

                for col_name, pattern in mapping.pattern.extensions.items():
                    print(lf.head(5).collect())
                    lf = lf.filter(
                        pl.col(col_name).str.contains(pattern)
                    )

                # extension columns
                lf = lf.with_columns(pl.lit(dataset).alias("dataset"))
                lf = lf.with_columns(pl.lit(version).alias("version"))
                lf = lf.with_columns(pl.lit(table).alias("table"))
                lf = lf.with_columns(pl.lit(event_name).alias("event"))
                for col_name, col_expr in concept.extension_columns.items():
                    lf = lf.with_columns(parse_expr(lf, col_expr).alias(col_name))

                # value columns
                if mapping.columns.text_value is None:
                    lf = lf.with_columns(pl.lit(None).alias("text_value"))
                else:
                    lf = lf.with_columns(
                        parse_expr(lf, mapping.columns.text_value).alias("text_value")
                    )

                if mapping.columns.numeric_value is None:
                    lf = lf.with_columns(pl.lit(None).alias("numeric_value"))
                else:
                    expr = parse_expr(lf, mapping.columns.numeric_value)

                    lf = lf.with_columns(
                        expr.cast(pl.Float64, strict=False).alias("numeric_value")
                    )

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

                lf = self.apply_limits(concept, lf)

                output_file = output_dataset_path / f"{str(uuid4())}.parquet"
                logger.debug(
                    "Writing temporary concept file for %s to %s",
                    concept.identifier,
                    output_file,
                )
                lf.sink_parquet(
                    output_file,
                )

                del lf
                gc.collect()

        files = list(output_dataset_path.glob("*.parquet"))
        if files:
            logger.info(
                "Writing merged concept file for %s to %s",
                concept.identifier,
                output_data_path / f"{dataset_concept.dataset}.parquet",
            )
            pl.scan_parquet(files).sink_parquet(output_data_path / f"{dataset_concept.dataset}.parquet")

        logger.debug(
            "Cleaning up temporary concept files for %s in %s",
            concept.identifier,
            output_dataset_path,
        )
        for file in files:
            file.unlink()

        output_dataset_path.rmdir()

    def get_path_for_concept_table(self, table: BaseConceptTable, dataset: str) -> Path:
        concept = self._registry.get(table.concept)
        if concept is None:
            raise FileNotFoundError(f"concept not found in registry ({table.concept})")
        assert self._workspace_dir is not None
        output_data_path = Path(self._workspace_dir.path, *concept.identifier_tuple[1:])
        return output_data_path / f"{dataset}.parquet"

    def extract_derived_concept(
        self,
        concept: ConceptConfig,
        dataset_concept: DerivedDatasetConceptConfig,
    ) -> None:
        logger.debug(
            "Extracting derived concept %s for dataset %s",
            concept.identifier,
            dataset_concept.dataset,
        )

        def _read_table(file_path: Path, table: BaseConceptTable) -> pl.LazyFrame:
            if not file_path.exists():
                raise FileNotFoundError(f"file not found ({file_path})")
            logger.debug(
                "Reading concept table %s from %s",
                table.concept,
                file_path,
            )
            lf = pl.scan_parquet(
                file_path,
                low_memory=True,
            ).select(table.columns)

            for expr in table.pre_callbacks:
                lf = lf.with_columns(parse_expr(lf, expr))

            for expr in table.callbacks:
                lf = lf.with_columns(parse_expr(lf, expr))

            return lf

        try:
            lf = _read_table(
                self.get_path_for_concept_table(dataset_concept.table, dataset_concept.dataset),
                dataset_concept.table,
            )
            post_callbacks = [*dataset_concept.table.post_callbacks]

            for join_table in dataset_concept.join:
                logger.debug(
                    "Joining concept table %s with %s (how=%s)",
                    dataset_concept.table.concept,
                    join_table.concept,
                    join_table.how,
                )
                lf = lf.join(
                    _read_table(
                        self.get_path_for_concept_table(join_table, dataset_concept.dataset),
                        join_table,
                    ),
                    how=join_table.how,  # ty: ignore[invalid-argument-type]
                    **join_table.join_params,  # ty: ignore[invalid-argument-type]
                )
                post_callbacks.extend(join_table.post_callbacks)
        except FileNotFoundError as e:
            logger.warning("skipping table %s: %s", dataset_concept.table.concept, e)
            return

        for expr in post_callbacks:
            lf = lf.with_columns(parse_expr(lf, expr))

        columns = dataset_concept.event.model_dump()
        extension = concept.extension_columns.copy()
        extension.update(columns.pop("extension") or {})
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
            lf = lf.with_columns(parse_expr(lf, col_expr).alias(col_name))

        # code column
        lf = lf.with_columns(pl.lit(concept.code).alias("code"))

        for expr in dataset_concept.filters:
            lf = lf.filter(parse_expr(lf, expr))

        # Reorder columns
        lf = lf.select([
            pl.col("subject_id").cast(pl.Int64),
            pl.col("time").cast(pl.Datetime(time_unit="us")),
            pl.col("code").cast(pl.String),
            pl.col("numeric_value").cast(pl.Float32),
            pl.col("text_value").cast(pl.String),
        ] + [pl.col(col).cast(pl.String) for col in extension.keys()])

        lf = self.apply_limits(concept, lf)

        assert self._workspace_dir is not None
        output_data_path = Path(self._workspace_dir.path, *concept.identifier_tuple[1:])
        output_data_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Writing derived concept %s to %s",
            concept.identifier,
            output_data_path / f"{dataset_concept.dataset}.parquet",
        )

        lf.sink_parquet(output_data_path / f"{dataset_concept.dataset}.parquet")

        del lf
        gc.collect()
