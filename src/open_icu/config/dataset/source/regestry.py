from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

import yaml

from open_icu.config.dataset.source.config.dataset import SourceDatasetConfig
from open_icu.config.dataset.source.config.table import TableConfig


class DatasetIdentifier(NamedTuple):
    name: str
    version: str


class DatasetConfigRegistry:
    def __init__(self, datasets: list[SourceDatasetConfig]):
        self._configs = {
            DatasetIdentifier(name=dataset.name, version=dataset.version): dataset
            for dataset in datasets
        }

    @classmethod
    def from_path(cls, path: str | Path | list[str | Path]) -> DatasetConfigRegistry:
        paths: list[Path]
        if isinstance(path, (str, Path)):
            paths = [Path(path)]
        elif isinstance(path, list):
            paths = [Path(p) for p in path]

        dataset_configs: dict[DatasetIdentifier, tuple[Path, dict]] = {}

        for path in paths:
            for dataset_config_path in path.rglob("dataset.*"):
                if not dataset_config_path.is_file():
                    continue

                if dataset_config_path.suffix.lower() not in {".yml", ".yaml"}:
                    continue

                with open(dataset_config_path, "r") as f:
                    config = yaml.safe_load(f)

                if (
                    (name := config.get("name")) is None or
                    (versions := config.get("versions")) is None
                ):
                    continue

                for version in versions:
                    if dataset_configs.get(DatasetIdentifier(name=name, version=version)) is None:
                        dataset_configs[DatasetIdentifier(name=name, version=version)] = (dataset_config_path, config)

        configs: list[SourceDatasetConfig] = []
        for dataset_identifier, (config_path, config) in dataset_configs.items():
            tables: list[TableConfig] = []
            for table_config_path in (config_path.parent / dataset_identifier.version).rglob("*.*"):
                if not table_config_path.is_file():
                    continue

                if table_config_path.suffix.lower() not in {".yml", ".yaml"}:
                    continue

                with open(table_config_path, "r") as f:
                    table_config = yaml.safe_load(f)

                tables.append(TableConfig(**table_config))

            configs.append(
                SourceDatasetConfig(
                    name=dataset_identifier.name,
                    version=dataset_identifier.version,
                    tables=tables,
                )
            )

        return cls(configs)

    def get(self, name: str, version: str) -> SourceDatasetConfig | None:
        return self._configs.get(DatasetIdentifier(name=name, version=version))

    def filter(self, name: str | None = None, version: str | None = None) -> list[SourceDatasetConfig]:
        return [
            config
            for identifier, config in self._configs.items()
            if (
                (name is None or identifier.name == name) and
                (version is None or identifier.version == version)
            )
        ]

    def all(self) -> list[SourceDatasetConfig]:
        return list(self._configs.values())
