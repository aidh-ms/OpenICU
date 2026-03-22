"""Concept step implementation for converting ICU data to MEDS format.

This module implements the ConceptStep class that orchestrates the extraction
of data from source CSV files, applies transformations via callbacks, performs
joins, and outputs MEDS-compliant Parquet files.
"""
import gc
from functools import cached_property
from graphlib import TopologicalSorter
from pathlib import Path
from uuid import uuid4

import polars as pl

from open_icu.callbacks.interpreter import parse_expr
from open_icu.config.registry import load_configs
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

    def setup_config(self) -> None:
        """Load external configuration files into the registry.

        Processes each ConfigFileConfig from the step configuration, loading
        YAML files into the registry with specified filtering and overwrite
        behavior. Saves the consolidated configuration to the project's
        configs directory.
        """
        dataset_paths = [
            dataset_config.path
            for dataset_config in self._config.config.dataset_configs
        ]

        for config in self._config.config_files:
            concepts = load_configs(
                config.path,
                ConceptConfig,
                includes=config.includes,
                excludes=config.excludes,
                dataset_paths=dataset_paths,
            )
            for concept in concepts:
                self._registry.register(concept, overwrite=config.overwrite)

        self._registry.save(self._project.configs_path)

    def extract(self) -> None:
        datasets = {
            dataset_config.name
            for dataset_config in self._config.config.dataset_configs
        }

        for dataset in datasets:
            depend_concepts = dict()

            for concept in self._registry.values():
                dataset_concept = concept.get_dataset_concept(dataset)
                if dataset_concept is None:
                    logger.warning(
                        "skipping concept %s for dataset %s: no dataset-specific config found",
                        concept.name,
                        dataset
                    )
                    continue

                if isinstance(dataset_concept, SimpleDatasetConceptConfig):
                    self.extract_simple_concept(
                        concept,
                        dataset_concept,
                    )

                if isinstance(dataset_concept, (DerivedDatasetConceptConfig, ComplexDatasetConceptConfig)):
                    depend_concepts[concept.identifier] = dataset_concept.dependencies

            for concept_id in TopologicalSorter(depend_concepts).static_order():
                concept = self._registry.get(concept_id)
                assert concept is not None

                if isinstance(dataset_concept, DerivedDatasetConceptConfig):
                    self.extract_derived_concept(
                        concept,
                        dataset_concept,
                    )

                if isinstance(dataset_concept, ComplexDatasetConceptConfig):
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

    def extract_simple_concept(
        self,
        concept: ConceptConfig,
        dataset_concept: SimpleDatasetConceptConfig,
    ) -> None:
        assert self._workspace_dir is not None
        output_data_path = Path(self._workspace_dir.path, *concept.identifier_tuple[1:])
        output_data_path.mkdir(parents=True, exist_ok=True)

        for mapping in dataset_concept.mappings:
            mapping_codes = self.codes_df.filter(pl.col("code").str.contains(mapping.regex))["code"]

            for dataset, version, table, event in mapping_codes.str.split("//").list.head(4).unique().to_list():
                data_path = self.extraction_dataset.data_path / dataset / version / table / f"{event}.parquet"
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

                output_file = output_data_path / f"{str(uuid4())}.parquet"
                lf.sink_parquet(
                    output_file,
                )

                del lf
                gc.collect()

        files = list(output_data_path.glob("*.parquet"))
        pl.scan_parquet(files).sink_parquet(output_data_path / f"{dataset_concept.dataset}.parquet")

        for file in files:
            file.unlink()

    def get_path_for_concept_table(self, table: BaseConceptTable, dataset: str) -> Path:
        concept_tuple = ConceptConfig.ensure_prefix(table.concept).split(("."))
        assert self._workspace_dir is not None
        output_data_path = Path(self._workspace_dir.path, *concept_tuple[1:])
        return output_data_path / f"{dataset}.parquet"

    def extract_derived_concept(
        self,
        concept: ConceptConfig,
        dataset_concept: DerivedDatasetConceptConfig,
    ) -> None:
        def _read_table(file_path: Path, table: BaseConceptTable) -> pl.LazyFrame:
            if not file_path.exists():
                raise FileNotFoundError(f"file not found ({file_path})")
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
                dataset_concept.table
            )
            post_callbacks = [*dataset_concept.table.post_callbacks]

            for join_table in dataset_concept.join:
                lf = lf.join(
                    _read_table(
                        self.get_path_for_concept_table(join_table, dataset_concept.dataset),
                        join_table
                    ),
                    how=join_table.how,  # type: ignore[invalid-argument-type]
                    **join_table.join_params,  # type: ignore[invalid-argument-type]
                )
                post_callbacks.extend(join_table.post_callbacks)
        except FileNotFoundError as e:
            logger.warning("skipping table %s: %s", dataset_concept.table.concept, e)
            return

        for expr in post_callbacks:
            lf = lf.with_columns(parse_expr(lf, expr))

        columns = dataset_concept.event.model_dump()
        extension = concept.extension_columns.copy()
        extension.update(columns.pop("extension"))
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

        assert self._workspace_dir is not None
        output_data_path = Path(self._workspace_dir.path, *concept.identifier_tuple[1:])
        output_data_path.mkdir(parents=True, exist_ok=True)
        lf.sink_parquet(output_data_path / f"{dataset_concept.dataset}.parquet")

        del lf
        gc.collect()
