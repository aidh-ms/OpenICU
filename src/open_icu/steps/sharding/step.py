"""Sharding step implementation for converting ICU data to MEDS format.

This module implements the ShardingStep class that orchestrates the extraction
of data from source CSV files, applies transformations via callbacks, performs
joins, and outputs MEDS-compliant Parquet files.
"""
from pathlib import Path

from open_icu.config.registry import load_configs
from open_icu.logging import get_logger
from open_icu.steps.base.step import ConfigurableBaseStep
from open_icu.steps.sharding.config.sharding import ShardingConfig
from open_icu.steps.sharding.config.step import ShardingStepConfig
from open_icu.steps.sharding.registry import sharding_config_registry
from open_icu.storage.project import OpenICUProject

logger = get_logger(__name__)

class ShardingStep(ConfigurableBaseStep[ShardingStepConfig, ShardingConfig]):
    """ TODO: Documentation
    """
    @classmethod
    def load(cls, project: OpenICUProject, config_path: Path) -> "ShardingStep":
        """Load a sharding step from a configuration file.

        Args:
            project: The OpenICU project to operate within
            config_path: Path to the sharding configuration YAML file

        Returns:
            An initialized ShardingStep instance
        """
        config = ShardingStepConfig.load(config_path)
        return cls(project, config, sharding_config_registry)

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
            shardings = load_configs(
                config.path,
                ShardingConfig,
                includes=config.includes,
                excludes=config.excludes,
                dataset_paths=dataset_paths,
            )
            for sharding in shardings:
                self._registry.register(sharding, overwrite=config.overwrite)

        self._registry.save(self._project.configs_path)

    def extract(self) -> None:
        pass
