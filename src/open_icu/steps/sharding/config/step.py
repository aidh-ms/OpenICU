from pathlib import Path

from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig
from open_icu.steps.sharding.config.selection import SelectionConfig


class PresetRefConfig(BaseModel):
    """Reference to a predefined sharding preset."""

    name: str = Field(
        ...,
        description="Name of the sharding preset.",
    )

    path: Path | None = Field(
        default=None,
        description="Optional path to the preset definition. If omitted, a default path is used.",
    )


class CustomConfig(BaseModel):
    """Custom configuration specific to the sharding step."""

    concept_step: str = Field(
        description="Name of the preceding concept step."
    )

    presets: list[PresetRefConfig] = Field(
        default_factory=list,
        description="References to sharding preset configurations to apply.",
    )

    concepts: SelectionConfig = Field(
        default_factory=SelectionConfig,
        description="Additional concept selection rules applied on top of presets.",
    )

    subjects: SelectionConfig = Field(
        default_factory=SelectionConfig,
        description="Subject selection rules.",
    )

    time_resolution: str | None = Field(
        default=None,
        description="Optional temporal resolution used for sharding output.",
    )

    presplit: bool = Field(
        default=False,
        description="Whether to split subjects before sharding.",
    )

    subjects_per_shard: int = Field(
        default=1000,
        gt=0,
        description="Number of subjects written per shard/file.",
    )


class ShardingStepConfig(BaseStepConfig[CustomConfig]):
    """Complete configuration for the sharding step."""

    pass
