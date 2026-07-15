"""Persistence step implementation.

The persistence step uses an extraction configuration to determine which
source CSV tables are required and writes those source tables as Parquet files.
The generated files are independent artifacts and are not used by the
extraction step.
"""

from pathlib import Path

import polars as pl
from polars.datatypes import DataTypeClass

from open_icu.logging import get_logger
from open_icu.steps.base.step import ConfigurableBaseStep
from open_icu.steps.extraction.config.step import ExtractionStepConfig
from open_icu.steps.extraction.config.table import (
    BaseTableConfig,
    TableConfig,
    TableType,
)
from open_icu.steps.extraction.registry import dataset_config_registry
from open_icu.storage.project import OpenICUProject

logger = get_logger(__name__)


class PersistenceStep(
    ConfigurableBaseStep[ExtractionStepConfig, TableConfig]
):
    """Persist source CSV tables selected by an extraction configuration."""

    @classmethod
    def load(
        cls,
        project: OpenICUProject,
        config_path: Path,
    ) -> "PersistenceStep":
        """Load the persistence step from an extraction configuration."""
        config = ExtractionStepConfig.load(config_path)

        # extraction.yml defines name: Extraction. Use a copied configuration
        # with a distinct step name so that this step gets its own workspace.
        config = config.model_copy(update={"name": "Persistence"})

        return cls(
            project,
            config,
            dataset_config_registry,
        )

    def extract(self) -> None:
        """Persist the source columns selected by the extraction configuration."""
        assert self._workspace_dir is not None

        for dataset in self._config.config.data:
            sources: dict[
                Path,
                tuple[str, dict[str, DataTypeClass]],
            ] = {}

            tables = self._registry.filter(
                dataset.name,
                dataset.version,
                includes=dataset.includes,
                excludes=dataset.excludes,
            )

            for table in tables:
                self._register_source(
                    sources=sources,
                    table=table,
                    table_name=table.name,
                    dataset_path=dataset.path,
                )

                for join_table in table.join:
                    self._register_source(
                        sources=sources,
                        table=join_table,
                        table_name=self._get_table_name(join_table.path),
                        dataset_path=dataset.path,
                    )

            for source_file, (table_name, dtypes) in sources.items():
                self._persist_table(
                    source_file=source_file,
                    table_name=table_name,
                    dtypes=dtypes,
                    dataset_name=dataset.name,
                    dataset_version=dataset.version,
                )

    def _persist_table(
        self,
        source_file: Path,
        table_name: str,
        dtypes: dict[str, DataTypeClass],
        dataset_name: str,
        dataset_version: str,
    ) -> None:
        """Persist configured source columns with their declared data types."""
        assert self._workspace_dir is not None

        if not dtypes:
            logger.warning(
                "Skipping source table '%s': no columns are configured",
                table_name,
            )
            return

        output_file = (
            self._workspace_dir.path
            / dataset_name
            / dataset_version
            / f"{table_name}.parquet"
        )
        output_file.parent.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Persisting %d configured columns from source table '%s' "
            "from %s to %s",
            len(dtypes),
            table_name,
            source_file,
            output_file,
        )

        pl.scan_csv(
            source_file,
            schema_overrides=dtypes,
            infer_schema=False,
            low_memory=True,
        ).select(
            list(dtypes),
        ).sink_parquet(
            output_file,
        )

    @staticmethod
    def _get_table_name(table_path: str) -> str:
        """Derive an output table name from a source file path."""
        filename = Path(table_path).name

        for suffix in (
            ".csv.gz",
            ".csv.bz2",
            ".csv.zip",
            ".csv",
            ".gz",
        ):
            if filename.lower().endswith(suffix):
                return filename[: -len(suffix)]

        return Path(filename).stem

    def collect(self) -> None:
        """Skip MEDS collection for persisted source tables."""
        logger.info(
            "Skipping MEDS collection for step '%s'; "
            "persisted source tables remain in the workspace",
            self._step_name,
        )

    def _register_source(
        self,
        sources: dict[
            Path,
            tuple[str, dict[str, DataTypeClass]],
        ],
        table: BaseTableConfig,
        table_name: str,
        dataset_path: Path,
    ) -> None:
        """Register the columns required from one source table."""
        if table.type not in {TableType.CSV, TableType.CSVGZ}:
            logger.debug(
                "Skipping non-CSV source table '%s'",
                table.path,
            )
            return

        source_file = (dataset_path / table.path).resolve()

        if not source_file.exists():
            logger.warning(
                "Skipping source table '%s': file not found at %s",
                table_name,
                source_file,
            )
            return

        if source_file not in sources:
            sources[source_file] = (
                table_name,
                dict(table.dtypes),
            )
            return

        existing_name, existing_dtypes = sources[source_file]

        for column_name, dtype in table.dtypes.items():
            existing_dtype = existing_dtypes.get(column_name)

            if existing_dtype is not None and existing_dtype != dtype:
                raise ValueError(
                    f"Conflicting dtypes for column '{column_name}' in "
                    f"source table '{source_file}': "
                    f"{existing_dtype} and {dtype}"
                )

            existing_dtypes[column_name] = dtype

        sources[source_file] = (
            existing_name,
            existing_dtypes,
        )
