"""Sharding step implementation for building subject-oriented shards from concept data.

This module implements the ShardingStep class, which loads reusable sharding
preset configurations, registers them in the sharding registry, and prepares
the configuration required to build subject-oriented shard outputs.
"""

from functools import cached_property
from pathlib import Path

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

    def extract(self) -> None:
        """Build subject-oriented shards from the configured concept data."""
        logger.info("Starting extract for sharding step '%s'", self._step_name)

        cnpt_paths = []
        concept_root = self._project.workspace_path / self._config.config.concept_step.lower()

        logger.info("Scanning concept parquet files under %s", concept_root)

        if not concept_root.exists():
            logger.warning(
                "Skipping sharding extract: concept root does not exist: %s",
                concept_root,
            )
            return

        total_found = 0
        skipped_invalid_structure = 0
        skipped_excluded = 0
        selected = 0

        for path in concept_root.rglob("*.parquet"):  # TODO: select Versions (mimic_demo, mimic_iv, ...)
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
            ds_name = parts[2]

            exclude = (
                ds_name in self._config.config.datasets.exclude
                or cnpt_name in self._config.config.concepts.exclude
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
                self._config.config.datasets.include_all
                or ds_name in self._config.config.datasets.include
            ) and (
                self._config.config.concepts.include_all
                or cnpt_name in self._config.config.concepts.include
            )

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
            (
                "Concept parquet scan finished: found=%d, selected=%d, "
                "skipped_invalid_structure=%d, skipped_excluded=%d"
            ),
            total_found,
            selected,
            skipped_invalid_structure,
            skipped_excluded,
        )

        if len(cnpt_paths) == 0:
            logger.warning(
                "Skipping sharding extract: no concept parquet files selected under %s",
                concept_root,
            )
            return

        logger.info("Building lazy frame from %d concept parquet file(s)", len(cnpt_paths))

        lf = pl.concat(
            [pl.scan_parquet(str(cnpt_path)) for cnpt_path in cnpt_paths],
            how="vertical",
        )

        logger.debug("Preparing long-format lazy transformation")

        # Long format vorbereiten
        lf_long = (
            lf.select(
                [
                    pl.col("subject_id"),
                    pl.col("time"),
                    # pl.col("hadm_id"),
                    # pl.col("stay_id"),
                    pl.col("code"),
                    pl.col("numeric_value"),
                ]
            )
            # .filter(pl.col("numeric_value").is_not_null())
            .group_by(
                [
                    "subject_id",
                    "time",
                    # "hadm_id",
                    # "stay_id",
                    "code",
                ]
            )
            .agg(pl.col("numeric_value").first().alias("numeric_value"))
        )

        logger.info("Collecting long-format dataframe")
        df_long = pl.DataFrame(lf_long.collect())
        logger.info(
            "Collected long-format dataframe with shape rows=%d, cols=%d",
            df_long.height,
            df_long.width,
        )

        logger.info("Pivoting long-format dataframe to wide format")
        df_wide = df_long.pivot(
            index=["subject_id", "time"],
            on="code",
            values="numeric_value",
            aggregate_function="first",
        ).sort(["subject_id", "time"])

        logger.info(
            "Built wide-format dataframe with shape rows=%d, cols=%d",
            df_wide.height,
            df_wide.width,
        )

        output_dir = self._project.workspace_path / self._step_name
        output_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Writing subject shards to %s", output_dir)

        groups = df_wide.partition_by("subject_id", as_dict=True)
        logger.info("Partitioned wide dataframe into %d subject shard(s)", len(groups))

        written_files = 0

        for subject_id, df_subject in groups.items():
            file_path = output_dir / f"subject_{subject_id[0]}.parquet"
            logger.debug(
                "Writing subject shard for subject_id=%s to %s with shape rows=%d, cols=%d",
                subject_id[0],
                file_path,
                df_subject.height,
                df_subject.width,
            )
            df_subject.write_parquet(file_path)
            written_files += 1

        logger.info(
            "Finished extract for sharding step '%s': wrote %d subject shard file(s)",
            self._step_name,
            written_files,
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
