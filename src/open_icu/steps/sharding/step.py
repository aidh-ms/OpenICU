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
        config = ShardingStepConfig.load(config_path)
        return cls(project, config, sharding_config_registry)

    def setup_config(self) -> None:
        """Load external sharding preset configurations into the registry.

        Processes each configured config file source, loads matching sharding
        preset configuration files, registers them in the sharding registry,
        and saves the merged registry state to the project's config directory.
        """
        for config in self._config.config_files:
            logger.debug(
                "Loading shardings from %s (overwrite=%s)",
                config.path,
                config.overwrite,
            )
            shardings = load_configs(
                config.path,
                ShardingConfig,
                includes=config.includes,
                excludes=config.excludes,
            )
            for sharding in shardings:
                logger.debug(
                    "Registering sharding '%s' (overwrite=%s)",
                    sharding.name,
                    config.overwrite,
                )
                self._registry.register(sharding, overwrite=config.overwrite)

        logger.info(
            "Saving merged configuration to %s",
            self._project.configs_path,
        )

        self._registry.save(self._project.configs_path)

    def extract(self) -> None:
        """Build subject-oriented shards from the configured concept data."""
        cnpt_paths = []
        concept_root = self._project.workspace_path / self._config.config.concept_step.lower()

        for path in concept_root.rglob("*.parquet"):  # TODO: select Versions (mimic_demo, mimic_iv, ...)
            parts = path.relative_to(concept_root).parts
            if len(parts) < 3:
                continue

            cnpt_name = parts[0]
            ds_name = parts[2]

            exclude = (
                ds_name in self._config.config.datasets.exclude or cnpt_name in self._config.config.concepts.exclude
            )

            if exclude:
                continue

            include = (
                self._config.config.datasets.include_all or cnpt_name in self._config.config.datasets.include
            ) and (self._config.config.concepts.include_all or cnpt_name in self._config.config.concepts.include)

            if include:
                cnpt_paths.append(path)

        if len(cnpt_paths) == 0:
            # TODO: log n oconcept parquet fiels selected
            return

        lf = pl.concat(
            [pl.scan_parquet(str(cnpt_path)) for cnpt_path in cnpt_paths],
            how="vertical",
        )

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

        # Erst hier materialisieren
        df_long = pl.DataFrame(lf_long.collect())

        # Dann eager pivot
        df_wide = df_long.pivot(
            index=["subject_id", "time"],
            on="code",
            values="numeric_value",
            aggregate_function="first",
        ).sort(["subject_id", "time"])

        output_dir = self._project.workspace_path / self._step_name
        output_dir.mkdir(parents=True, exist_ok=True)

        groups = df_wide.partition_by("subject_id", as_dict=True)

        for subject_id, df_subject in groups.items():
            file_path = output_dir / f"subject_{subject_id[0]}.parquet"
            df_subject.write_parquet(file_path)

    @property
    def concept_dataset(self):
        concept_dataset = self._project.datasets.get(self._config.config.concept_step.lower())
        if not concept_dataset:
            logger.warning("skipping sharding step:  dataset not found")
            return
        return self._project.datasets.get(self._config.config.concept_step.lower())

    @cached_property
    def codes_df(self) -> pl.DataFrame:
        concept_dataset = self.concept_dataset
        if not concept_dataset:
            logger.warning("skipping concept step: extraction dataset not found")
            return pl.DataFrame()
        codes_path = concept_dataset.metadata_path / "codes.parquet"
        if not codes_path.exists():
            logger.warning("skipping concept step: extraction codes.parquet not found")
            return pl.DataFrame()
        return pl.read_parquet(codes_path)

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

        # for file_path in self._workspace_dir.content:
        #     relative_path = file_path.relative_to(self._workspace_dir._path)
        #     dest_path = self._dataset.data_path / relative_path
        #     dest_path.parent.mkdir(parents=True, exist_ok=True)

        #     logger.debug("Copying %s -> %s", file_path, dest_path)

        #     shutil.copy(file_path, dest_path)

        # self._dataset.write_metadata(self._config.dataset.metadata)
        # self._dataset.write_codes()                                                               # TODO: Need sth similar

        logger.info(
            "Finished collecting results for step '%s'",
            self._step_name,
        )
