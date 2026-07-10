"""Sharding step implementation.

The sharding step takes the long-format concept output produced by the concept
step and rewrites it into subject-oriented Parquet shard files. It deliberately
keeps the output long-format; wide exports such as YAIB belong in separate
export/adapter tooling.
"""

from pathlib import Path

import polars as pl

from open_icu.logging import get_logger
from open_icu.steps.base.step import ConfigurableBaseStep
from open_icu.steps.sharding.config.sharding import ShardingConfig
from open_icu.steps.sharding.config.step import ShardingStepConfig
from open_icu.steps.sharding.registry import sharding_config_registry
from open_icu.storage.project import OpenICUProject

logger = get_logger(__name__)

class ShardingStep(ConfigurableBaseStep[ShardingStepConfig, ShardingConfig]):
    """Create subject-oriented long-format shards from concept Parquet files."""

    @classmethod
    def load(cls, project: OpenICUProject, config_path: Path) -> "ShardingStep":
        """Load a sharding step from a YAML configuration file."""
        config = ShardingStepConfig.load(config_path)
        return cls(project, config, sharding_config_registry)

    @property
    def concept_dataset(self):
        """Return the dataset produced by the configured concept step."""
        concept_step_name = self._config.config.concept_step.lower()
        concept_dataset = self._project.datasets.get(concept_step_name)
        if concept_dataset is None:
            logger.warning("Skipping sharding step: concept dataset '%s' not found", concept_step_name)
        return concept_dataset

    def extract(self) -> None:
        """Build subject-oriented shard files from selected concept files."""
        assert self._workspace_dir is not None

        concept_dataset = self.concept_dataset
        if concept_dataset is None:
            return

        concept_files = self._selected_concept_files(concept_dataset.data_path)
        if not concept_files:
            logger.warning("Skipping sharding step: no concept files found below %s", concept_dataset.data_path)
            return

        logger.info("Sharding %d concept file(s)", len(concept_files))

        subject_ids = self._subject_ids(concept_files)
        if not subject_ids:
            logger.warning("Skipping sharding step: no subjects found in selected concept files")
            return

        written_files = 0
        for shard_idx, shard_subjects in enumerate(self._chunks(subject_ids, self._config.config.subjects_per_shard)):
            output_file = self._workspace_dir.path / f"shard_{shard_idx:05d}.parquet"
            logger.info(
                "Writing shard %s with %d subject(s) to %s",
                shard_idx,
                len(shard_subjects),
                output_file,
            )

            lf = self._scan_core_columns(concept_files).filter(pl.col("subject_id").is_in(shard_subjects))
            lf = lf.sort(["subject_id", "time", "code"])
            lf.sink_parquet(output_file)
            written_files += 1

        logger.info("Finished sharding step: wrote %d shard file(s)", written_files)

    def _selected_concept_files(self, concept_data_path: Path) -> list[Path]:
        """Find concept Parquet files matching the configured dataset/concept filters."""
        datasets = set(self._config.config.datasets)
        concept_filters = self._normalize_concept_filters(self._config.config.concepts)

        concept_files: list[Path] = []
        for file_path in sorted(concept_data_path.rglob("*.parquet")):
            if datasets and file_path.stem not in datasets:
                continue

            relative_file_path = file_path.relative_to(concept_data_path)
            relative_parts = relative_file_path.parts

            if len(relative_parts) < 3:
                logger.warning(
                    "Skipping concept file with unexpected path structure: %s",
                    file_path,
                )
                continue

            concept_path = Path(*relative_parts[:-2])

            if concept_filters and not self._matches_concept_filter(
                concept_path,
                concept_filters,
            ):
                continue

            concept_files.append(file_path)

        return concept_files

    @staticmethod
    def _normalize_concept_filters(concepts: list[str]) -> set[str]:
        return {concept.replace("\\", "/").strip("/").lower() for concept in concepts}

    @staticmethod
    def _matches_concept_filter(concept_path: Path, concept_filters: set[str]) -> bool:
        concept_path_str = concept_path.as_posix().lower()
        concept_name = concept_path.name.lower()
        return concept_path_str in concept_filters or concept_name in concept_filters

    def _subject_ids(self, concept_files: list[Path]) -> list[int]:
        """Collect selected subject IDs from the selected concept files."""
        lfs = [
            pl.scan_parquet(file_path).select(pl.col("subject_id").cast(pl.Int64))
            for file_path in concept_files
        ]
        lf = pl.concat(lfs, how="vertical").unique().sort("subject_id")

        configured_subjects = self._config.config.subjects
        if configured_subjects:
            lf = lf.filter(pl.col("subject_id").is_in(configured_subjects))

        return lf.collect(engine="streaming")["subject_id"].to_list()

    @staticmethod
    def _scan_core_columns(concept_files: list[Path]) -> pl.LazyFrame:
        """Scan all selected concept files using the stable long-format columns."""
        lfs = [
            pl.scan_parquet(file_path).select(
                [
                    pl.col("subject_id").cast(pl.Int64),
                    pl.col("time").cast(pl.Datetime(time_unit="us")),
                    pl.col("code").cast(pl.String),
                    pl.col("numeric_value").cast(pl.Float32),
                    pl.col("text_value").cast(pl.String),
                ]
            )
            for file_path in concept_files
        ]
        return pl.concat(lfs, how="vertical")

    @staticmethod
    def _chunks(values: list[int], chunk_size: int):
        for start in range(0, len(values), chunk_size):
            yield values[start : start + chunk_size]
