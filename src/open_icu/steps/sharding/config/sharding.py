"""Sharding configuration models.

This module defines the configuration type managed by the sharding
configuration registry.
"""

from open_icu.config.base import BaseConfig


class ShardingConfig(BaseConfig):
    """Configuration object managed by the sharding registry."""

    __open_icu_config_type__ = "sharding"
