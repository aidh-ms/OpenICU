"""Registry for sharding configurations.

This module provides a singleton registry for managing ShardingConfig objects
used in the sharding step. The global sharding_config_registry instance
stores all loaded sharding configurations.
"""

from open_icu.config.registry import BaseConfigRegistry
from open_icu.steps.sharding.config.sharding import ShardingConfig


class ShardingConfigRegistry(BaseConfigRegistry[ShardingConfig]):
    """Registry for sharding configuration objects.

    Specialized registry for storing and retrieving ShardingConfig instances
    that define how to shard data from config tables and transform them
    into MEDS events.
    """
    pass

sharding_config_registry = ShardingConfigRegistry()
"""Global singleton instance of the concept configuration registry."""
