"""Sharding step implementation for building subject-oriented shards from concept data.

This module implements the ShardingStep class, which loads reusable sharding
preset configurations, registers them in the sharding registry, and prepares
the configuration required to build subject-oriented shard outputs.
"""

import shutil
from functools import cached_property
from pathlib import Path
from typing import cast

import polars as pl

from open_icu.config.registry import load_configs
from open_icu.logging import get_logger
from open_icu.steps.base.step import ConfigurableBaseStep
from open_icu.steps.sharding.config.sharding import ShardingConfig
from open_icu.steps.sharding.config.step import ShardingStepConfig
from open_icu.steps.sharding.registry import sharding_config_registry
from open_icu.storage.project import OpenICUProject

logger = get_logger(__name__)


class ShardingStep(ConfigurableBaseStep[ShardingStepConfig, ShardingConfig]):
    """Sharding step for creating subject-oriented shard outputs from concept data."""

    # Tune this if needed.
    # More buckets -> lower peak memory per bucket, but more files / overhead.
    _DEFAULT_BUCKET_COUNT = 256

    @classmethod
    def load(cls, project: OpenICUProject, config_path: Path) -> "ShardingStep":
        """Load a sharding step from a configuration file.

        Args:
            project: The OpenICU project to operate within.
            config_path: Path to the sharding step configuration YAML file.

        Returns:
            An initialized ShardingStep instance.
        """
        logger.info("Loading sharding step configuration from %s", config_path)
        config = ShardingStepConfig.load(config_path)
        logger.debug("Loaded sharding step config with step name '%s'", config.name)
        return cls(project, config, sharding_config_registry)

    def setup_config(self) -> None:
        """Load external sharding preset configurations into the registry.

        Processes each configured config file source, loads matching sharding
        preset configuration files, registers them in the sharding registry,
        and saves the merged registry state to the project's config directory.
        """
        logger.info("Setting up sharding configuration registry for step '%s'", self._step_name)

        total_loaded = 0
        total_registered = 0

        for config in self._config.config_files:
            logger.debug(
                "Loading shardings from %s (overwrite=%s, includes=%s, excludes=%s)",
                config.path,
                config.overwrite,
                config.includes,
                config.excludes,
            )
            shardings = load_configs(
                config.path,
                ShardingConfig,
                includes=config.includes,
                excludes=config.excludes,
            )

            logger.info(
                "Loaded %d sharding preset(s) from %s",
                len(shardings),
                config.path,
            )
            total_loaded += len(shardings)

            for sharding in shardings:
                logger.debug(
                    "Registering sharding '%s' (overwrite=%s)",
                    sharding.name,
                    config.overwrite,
                )
                self._registry.register(sharding, overwrite=config.overwrite)
                total_registered += 1

        logger.info(
            "Saving merged sharding configuration to %s",
            self._project.configs_path,
        )
        self._registry.save(self._project.configs_path)

        logger.info(
            "Finished setup_config for step '%s': loaded=%d, registered=%d",
            self._step_name,
            total_loaded,
            total_registered,
        )

    def _select_concept_paths(self, concept_root: Path) -> list[Path]:
        """Select concept parquet paths based on include/exclude configuration."""
        logger.info("Scanning concept parquet files under %s", concept_root)

        if not concept_root.exists():
            logger.warning(
                "Skipping sharding extract: concept root does not exist: %s",
                concept_root,
            )
            return []

        cnpt_paths: list[Path] = []

        total_found = 0
        skipped_invalid_structure = 0
        skipped_excluded = 0
        selected = 0

        for path in concept_root.rglob("*.parquet"):  # TODO: select versions explicitly
            total_found += 1
            parts = path.relative_to(concept_root).parts

            if len(parts) < 3:
                skipped_invalid_structure += 1
                logger.debug(
                    "Skipping parquet with unexpected path structure: %s (parts=%s)",
                    path,
                    parts,
                )
                continue

            cnpt_name = parts[0]
            ds_name = parts[2].split(".")[0]

            exclude = (
                ds_name in self._config.config.datasets.exclude or cnpt_name in self._config.config.concepts.exclude
            )

            if exclude:
                skipped_excluded += 1
                logger.debug(
                    "Excluding parquet %s (concept=%s, dataset=%s)",
                    path,
                    cnpt_name,
                    ds_name,
                )
                continue

            include = (
                self._config.config.datasets.include_all or ds_name in self._config.config.datasets.include
            ) and (self._config.config.concepts.include_all or cnpt_name in self._config.config.concepts.include)

            if include:
                cnpt_paths.append(path)
                selected += 1
                logger.debug(
                    "Selected parquet %s (concept=%s, dataset=%s)",
                    path,
                    cnpt_name,
                    ds_name,
                )
            else:
                logger.debug(
                    "Skipping parquet not matched by include rules: %s (concept=%s, dataset=%s)",
                    path,
                    cnpt_name,
                    ds_name,
                )

        logger.info(
            ("Concept parquet scan finished: found=%d, selected=%d, skipped_invalid_structure=%d, skipped_excluded=%d"),
            total_found,
            selected,
            skipped_invalid_structure,
            skipped_excluded,
        )

        return cnpt_paths

    def _prepare_directories(self, output_dir: Path) -> tuple[Path, Path]:
        """Prepare output and temporary bucket directories."""
        temp_root = output_dir / "_tmp"
        bucket_root = temp_root / "buckets"

        if temp_root.exists():
            logger.info("Removing previous temporary sharding directory %s", temp_root)
            shutil.rmtree(temp_root)

        output_dir.mkdir(parents=True, exist_ok=True)
        bucket_root.mkdir(parents=True, exist_ok=True)

        return temp_root, bucket_root

    def _bucket_id_expr(self, bucket_count: int) -> pl.Expr:
        """Return a stable bucket id expression for subject_id."""
        return pl.col("subject_id").mod(bucket_count).alias("_bucket_id")

    def _write_long_bucket_chunks(
        self,
        cnpt_paths: list[Path],
        bucket_root: Path,
        bucket_count: int,
    ) -> int:
        """Read concept files one by one and write long-format bucket chunks.

        This avoids building one global dataframe for all concept files.
        """
        logger.info(
            "Writing long-format bucket chunks from %d concept parquet file(s) into %d bucket(s)",
            len(cnpt_paths),
            bucket_count,
        )

        chunk_counter = 0
        total_rows_written = 0

        for file_index, cnpt_path in enumerate(cnpt_paths, start=1):
            logger.info(
                "Processing concept parquet %d/%d: %s",
                file_index,
                len(cnpt_paths),
                cnpt_path,
            )

            # Keep each input file isolated in memory.
            lf_file = (
                pl.scan_parquet(str(cnpt_path))
                .select(
                    [
                        pl.col("subject_id"),
                        pl.col("time"),
                        pl.col("code"),
                        pl.col("numeric_value"),
                    ]
                )
                .filter(pl.col("subject_id").is_not_null())
                .filter(pl.col("time").is_not_null())
                .filter(pl.col("code").is_not_null())
                .with_columns(self._bucket_id_expr(bucket_count))
            )

            logger.debug("Collecting reduced dataframe for %s", cnpt_path)
            df_file = cast(pl.DataFrame, lf_file.collect(background=False))

            if df_file.is_empty():
                logger.debug("Skipping empty dataframe for %s", cnpt_path)
                continue

            logger.info(
                "Collected reduced dataframe for %s with shape rows=%d, cols=%d",
                cnpt_path,
                df_file.height,
                df_file.width,
            )

            # Partition only this single source file, not the whole dataset.
            groups = df_file.partition_by("_bucket_id", as_dict=True)
            logger.debug(
                "Split %s into %d bucket partition(s)",
                cnpt_path,
                len(groups),
            )

            for bucket_key, df_bucket in groups.items():
                bucket_id = int(bucket_key[0])
                bucket_dir = bucket_root / f"bucket={bucket_id:04d}"
                bucket_dir.mkdir(parents=True, exist_ok=True)

                chunk_path = bucket_dir / f"chunk_{chunk_counter:08d}.parquet"

                df_bucket = df_bucket.drop("_bucket_id")
                df_bucket.write_parquet(chunk_path, compression="zstd")

                total_rows_written += df_bucket.height
                chunk_counter += 1

                logger.debug(
                    "Wrote bucket chunk %s with shape rows=%d, cols=%d",
                    chunk_path,
                    df_bucket.height,
                    df_bucket.width,
                )

        logger.info(
            "Finished writing bucket chunks: chunks=%d, rows=%d",
            chunk_counter,
            total_rows_written,
        )
        return chunk_counter

    def _ordered_code_columns(self) -> list[str]:
        """Return code columns in preferred metadata order if available."""
        df = self.codes_df
        if df.is_empty():
            return []

        if "code" not in df.columns:
            logger.warning("codes.parquet does not contain a 'code' column; skipping code-based ordering")
            return []

        codes = [code for code in df.get_column("code").to_list() if isinstance(code, str) and code]
        logger.debug("Resolved %d ordered code column(s) from metadata", len(codes))
        return codes

    def _drop_all_null_value_columns(self, df_subject: pl.DataFrame) -> pl.DataFrame:
        """Keep index columns and only value columns that contain at least one non-null value."""
        fixed_columns = ["subject_id", "time"]
        value_columns = [col for col in df_subject.columns if col not in fixed_columns]

        if not value_columns:
            return df_subject

        non_empty_value_columns = [
            col for col in value_columns if df_subject.get_column(col).null_count() < df_subject.height
        ]

        return df_subject.select(fixed_columns + non_empty_value_columns)

    def _process_bucket_to_subject_files(
        self,
        bucket_dir: Path,
        output_dir: Path,
        ordered_codes: list[str],
    ) -> int:
        """Read one bucket, deduplicate, pivot, and write one file per subject."""
        chunk_paths = sorted(bucket_dir.glob("*.parquet"))
        if not chunk_paths:
            logger.debug("Skipping empty bucket directory %s", bucket_dir)
            return 0

        logger.info(
            "Processing bucket %s with %d chunk file(s)",
            bucket_dir.name,
            len(chunk_paths),
        )

        lf_bucket = (
            pl.concat([pl.scan_parquet(str(path)) for path in chunk_paths], how="vertical")
            .group_by(["subject_id", "time", "code"])
            .agg(pl.col("numeric_value").first().alias("numeric_value"))
        )

        logger.info("Collecting deduplicated long dataframe for bucket %s", bucket_dir.name)
        df_long = cast(pl.DataFrame, lf_bucket.collect(background=False))

        if df_long.is_empty():
            logger.info("Bucket %s is empty after deduplication", bucket_dir.name)
            return 0

        logger.info(
            "Collected bucket %s long dataframe with shape rows=%d, cols=%d",
            bucket_dir.name,
            df_long.height,
            df_long.width,
        )

        logger.info("Pivoting bucket %s to wide format", bucket_dir.name)
        df_wide = df_long.pivot(
            index=["subject_id", "time"],
            on="code",
            values="numeric_value",
            aggregate_function="first",
        ).sort(["subject_id", "time"])

        logger.info(
            "Built bucket %s wide dataframe with shape rows=%d, cols=%d",
            bucket_dir.name,
            df_wide.height,
            df_wide.width,
        )

        fixed_columns = ["subject_id", "time"]
        existing_value_columns = [col for col in df_wide.columns if col not in fixed_columns]

        if ordered_codes:
            ordered_existing_value_columns = [col for col in ordered_codes if col in existing_value_columns]
            remaining_value_columns = [
                col for col in existing_value_columns if col not in set(ordered_existing_value_columns)
            ]
            df_wide = df_wide.select(fixed_columns + ordered_existing_value_columns + remaining_value_columns)

        groups = df_wide.partition_by("subject_id", as_dict=True)
        logger.info(
            "Partitioned bucket %s into %d subject dataframe(s)",
            bucket_dir.name,
            len(groups),
        )

        written_files = 0

        for subject_key, df_subject in groups.items():
            subject_id = subject_key[0]
            df_subject = self._drop_all_null_value_columns(df_subject)

            file_path = output_dir / f"subject_{subject_id}.parquet"
            logger.debug(
                "Writing subject shard for subject_id=%s to %s with shape rows=%d, cols=%d",
                subject_id,
                file_path,
                df_subject.height,
                df_subject.width,
            )
            df_subject.write_parquet(file_path, compression="zstd")
            written_files += 1

        logger.info(
            "Finished processing bucket %s: wrote %d subject file(s)",
            bucket_dir.name,
            written_files,
        )
        return written_files

    def extract(self) -> None:
        """Build subject-oriented shards from the configured concept data."""
        logger.info("Starting extract for sharding step '%s'", self._step_name)

        concept_root = self._project.workspace_path / self._config.config.concept_step.lower()
        output_dir = self._project.workspace_path / self._step_name

        cnpt_paths = self._select_concept_paths(concept_root)
        if not cnpt_paths:
            logger.warning(
                "Skipping sharding extract: no concept parquet files selected under %s",
                concept_root,
            )
            return

        temp_root, bucket_root = self._prepare_directories(output_dir)

        bucket_count = self._DEFAULT_BUCKET_COUNT
        logger.info(
            "Using %d subject bucket(s) for sharding step '%s'",
            bucket_count,
            self._step_name,
        )

        chunk_count = self._write_long_bucket_chunks(
            cnpt_paths=cnpt_paths,
            bucket_root=bucket_root,
            bucket_count=bucket_count,
        )

        if chunk_count == 0:
            logger.warning(
                "Skipping sharding extract: no bucket chunks were written for step '%s'",
                self._step_name,
            )
            if temp_root.exists():
                shutil.rmtree(temp_root)
            return

        ordered_codes = self._ordered_code_columns()

        total_written_files = 0
        bucket_dirs = sorted(path for path in bucket_root.iterdir() if path.is_dir())

        logger.info(
            "Processing %d bucket directory/directories into final subject shards",
            len(bucket_dirs),
        )

        for bucket_dir in bucket_dirs:
            total_written_files += self._process_bucket_to_subject_files(
                bucket_dir=bucket_dir,
                output_dir=output_dir,
                ordered_codes=ordered_codes,
            )

        if temp_root.exists():
            logger.info("Removing temporary sharding directory %s", temp_root)
            shutil.rmtree(temp_root)

        logger.info(
            "Finished extract for sharding step '%s': buckets=%d, chunks=%d, subject_files=%d",
            self._step_name,
            len(bucket_dirs),
            chunk_count,
            total_written_files,
        )

    @property
    def concept_dataset(self):
        logger.debug(
            "Resolving concept dataset for concept step '%s'",
            self._config.config.concept_step.lower(),
        )
        concept_dataset = self._project.datasets.get(self._config.config.concept_step.lower())
        if not concept_dataset:
            logger.warning("Skipping sharding step: concept dataset not found")
            return None

        logger.debug(
            "Resolved concept dataset '%s' with metadata path %s",
            self._config.config.concept_step.lower(),
            concept_dataset.metadata_path,
        )
        return concept_dataset

    @cached_property
    def codes_df(self) -> pl.DataFrame:
        logger.debug("Loading codes dataframe for sharding step '%s'", self._step_name)

        concept_dataset = self.concept_dataset
        if not concept_dataset:
            logger.warning("Skipping codes dataframe load: concept dataset not found")
            return pl.DataFrame()

        codes_path = concept_dataset.metadata_path / "codes.parquet"
        logger.debug("Looking for codes parquet at %s", codes_path)

        if not codes_path.exists():
            logger.warning("Skipping codes dataframe load: codes.parquet not found at %s", codes_path)
            return pl.DataFrame()

        df = pl.read_parquet(codes_path)
        logger.info(
            "Loaded codes dataframe from %s with shape rows=%d, cols=%d",
            codes_path,
            df.height,
            df.width,
        )
        return df

    def collect(self) -> None:
        """Collect workspace results into the final MEDS dataset.

        Copies all Parquet files from the workspace directory to the dataset's
        data directory, then writes dataset metadata and code vocabulary files
        to complete the MEDS-compliant output.
        """
        if self._workspace_dir is None or self._dataset is None:
            logger.debug(
                "Skipping collect step '%s': workspace or dataset not initialized",
                self._step_name,
            )
            return

        logger.info(
            "Collecting results for step '%s' into dataset at %s",
            self._step_name,
            self._dataset.data_path,
        )

        workspace_files = list(self._workspace_dir.content)
        logger.info(
            "Workspace for step '%s' contains %d file(s)",
            self._step_name,
            len(workspace_files),
        )

        # for file_path in workspace_files:
        #     relative_path = file_path.relative_to(self._workspace_dir._path)
        #     dest_path = self._dataset.data_path / relative_path
        #     dest_path.parent.mkdir(parents=True, exist_ok=True)
        #
        #     logger.debug("Copying %s -> %s", file_path, dest_path)
        #
        #     shutil.copy(file_path, dest_path)

        logger.debug(
            "Collect copy phase is currently disabled for step '%s'",
            self._step_name,
        )

        # self._dataset.write_metadata(self._config.dataset.metadata)
        # self._dataset.write_codes()  # TODO: Need sth similar

        logger.debug(
            "Metadata/code writing is currently disabled for step '%s'",
            self._step_name,
        )

        logger.info(
            "Finished collecting results for step '%s'",
            self._step_name,
        )
