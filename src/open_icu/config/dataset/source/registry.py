from __future__ import annotations

from pathlib import Path
import logging

import yaml

from open_icu.config.dataset.source.config.dataset import SourceDatasetConfig
from open_icu.config.dataset.source.config.table import TableConfig
from open_icu.helper.registry import BaseConfigRegistry
from open_icu.metrics.metrics import get_statistics, PipelineArtifacts as pa

statistics = get_statistics()
logger = logging.getLogger(__name__) 

class DatasetConfigRegistry(BaseConfigRegistry[SourceDatasetConfig]):
    config_class = SourceDatasetConfig
    config_file_name = "dataset.*"

    def _parse_configs(self, paths: list[Path]) -> list[SourceDatasetConfig]:
        """Parse dataset configuration files into list of SourceDatasetConfig instances."""
        configs: list[SourceDatasetConfig] = []
        for path in paths:
            with open(path, "r") as f:
                config = yaml.safe_load(f)

            if (
                (name := config.get("name")) is None or
                (versions := config.get("versions")) is None
            ):
                continue

            for version in versions:
                tables: list[TableConfig] = []
                for table_config_path in (path.parent / version).rglob("*.*"):
                    if not table_config_path.is_file():
                        continue

                    if table_config_path.suffix.lower() not in {".yml", ".yaml"}:
                        continue

                    with open(table_config_path, "r") as f:
                        table_config = yaml.safe_load(f)
                    statistics.add(str(pa.SRC_CONFIG_AVAILABLE), table_config["name"])
                    tables.append(TableConfig(**table_config))

                configs.append(
                    SourceDatasetConfig(
                        name=name,
                        version=version,
                        tables=tables,
                    )
                )

        logger.info(f"Loaded {len(configs)} = {statistics.to_dict()} srcconfig files")
        statistics.save()
        return configs
