"""Persistence step implementation.

The persistence step reads selected source CSV files and writes equivalent
Parquet files to the project workspace. The generated Parquet files are
independent artifacts and are not used as input by subsequent pipeline steps.
"""

from pathlib import Path

import polars as pl

from open_icu.config.base import BaseConfig
from open_icu.config.registry import BaseConfigRegistry
from open_icu.logging import get_logger
from open_icu.steps.base.step import ConfigurableBaseStep
from open_icu.steps.persistence.config.step import PersistenceStepConfig
from open_icu.storage.project import OpenICUProject

logger = get_logger(__name__)


class PersistenceConfig(BaseConfig):
    """Placeholder configuration type for the persistence step registry."""

    __open_icu_config_type__ = "persistence"


class PersistenceConfigRegistry(BaseConfigRegistry[PersistenceConfig]):
    """Registry used by the persistence step."""

    pass


persistence_config_registry = PersistenceConfigRegistry()


class PersistenceStep(
    ConfigurableBaseStep[PersistenceStepConfig, PersistenceConfig]
):
    """Persist selected source CSV tables as Parquet files."""

    @classmethod
    def load(
        cls,
        project: OpenICUProject,
        config_path: Path,
    ) -> "PersistenceStep":
        """Load a persistence step from a YAML configuration file."""
        config = PersistenceStepConfig.load(config_path)
        return cls(project, config, persistence_config_registry)

    def extract(self) -> None:
        """Write all configured source CSV tables as Parquet files."""
        assert self._workspace_dir is not None

        for dataset in self._config.config.data:
            for table in dataset.tables:
                self._persist_table(
                    dataset_name=dataset.name,
                    dataset_version=dataset.version,
                    dataset_path=dataset.path,
                    table_name=table.name,
                    table_path=table.path,
                )

    def _persist_table(
        self,
        dataset_name: str,
        dataset_version: str,
        dataset_path: Path,
        table_name: str,
        table_path: Path,
    ) -> None:
        """Read one source CSV file and write it as a Parquet file."""
        source_file = dataset_path / table_path


        assert self._workspace_dir is not None
        output_file = (
            self._workspace_dir.path
            / dataset_name
            / dataset_version
            / f"{table_name}.parquet"
        )
        output_file.parent.mkdir(parents=True, exist_ok=True)

        if not source_file.exists():
            logger.warning(
                "Skipping source table '%s': file not found at %s",
                table_name,
                source_file,
            )
            return

        logger.info(
            "Persisting source table '%s' from %s to %s",
            table_name,
            source_file,
            output_file,
        )

        pl.scan_csv(source_file).sink_parquet(output_file)

    def collect(self) -> None:
        """Skip MEDS collection for persisted source tables."""
        logger.info(
            "Skipping MEDS collection for step '%s'; "
            "persisted source tables remain in the workspace",
            self._step_name,
        )
