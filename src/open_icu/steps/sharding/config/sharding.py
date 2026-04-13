from pydantic import Field

from open_icu.config.base import BaseConfig
from open_icu.steps.sharding.config.selection import SelectionConfig


class ShardingConfig(BaseConfig):
    """Configuration for a reusable sharding preset."""

    __open_icu_config_type__ = "sharding"

    description: str | None = Field(
        default=None,
        description="Optional description of the sharding preset.",
    )

    selection: SelectionConfig = Field(
        default_factory=SelectionConfig,
        description="Selection rules defined by this sharding preset.",
    )
