"""Sharding step implementation for converting ICU data to MEDS format.

This module implements the ShardingStep class that orchestrates the extraction
of data from source CSV files, applies transformations via callbacks, performs
joins, and outputs MEDS-compliant Parquet files.
"""
from pathlib import Path

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

    def extract(self) -> None:
        pass
