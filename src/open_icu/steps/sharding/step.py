"""Sharding step implementation for building subject-oriented shards from concept data.

This module implements the ShardingStep class, which loads reusable sharding
preset configurations, registers them in the sharding registry, and prepares
the configuration required to build subject-oriented shard outputs.
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
        pass
