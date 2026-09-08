"""Configuration models for the sharding step."""

from pathlib import Path
from typing import Self

from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig


class CustomConfig(BaseModel):
    """Configuration specific to the sharding step.

    The sharding step reads the output of a preceding concept step and writes
    long-format Parquet shards grouped by subject. Empty dataset, concept, or
    subject lists mean "include all".

    Attributes:
        concept_step: Name of the preceding concept step whose output should be
            sharded.
        datasets: Dataset names to include. An empty list includes all
            available datasets.
        concepts: Concept names or relative concept paths to include. An empty
            list includes all available concepts.
        subjects: Subject IDs to include. An empty list includes all available
            subjects.
        subjects_per_shard: Maximum number of subjects written to each shard
            file.
        event_order_config: Optional path to a custom global event-order
            configuration. If omitted, OpenICU uses its built-in default.
    """

    concept_step: str = Field(
        ...,
        description="Name of the preceding concept step whose dataset should be sharded.",
    )
    datasets: list[str] = Field(
        default_factory=list,
        description="Dataset names to include, e.g. mimic-iv. Empty means all datasets.",
    )
    concepts: list[str] = Field(
        default_factory=list,
        description=(
            "Concept names or relative concept paths to include. "
            "Examples: heart_rate, vital/heart_rate. Empty means all concepts."
        ),
    )
    subjects: list[int] = Field(
        default_factory=list,
        description="Subject IDs to include. Empty means all subjects.",
    )
    subjects_per_shard: int = Field(
        default=1000,
        gt=0,
        description="Number of subjects written per shard file.",
    )
    event_order_config: Path | None = Field(
        default=None,
        description=(
            "Optional path to a custom event-order configuration. "
            "Relative paths are resolved relative to this sharding configuration file. "
            "If omitted, OpenICU uses the built-in default event order."
        ),
    )


class ShardingStepConfig(BaseStepConfig[CustomConfig]):
    """Complete configuration for the sharding step."""

    @classmethod
    def load(cls, file_path: Path, **kwargs) -> Self:
        """Load the sharding configuration and resolve relative file paths."""
        config = super().load(file_path, **kwargs)

        event_order_path = config.config.event_order_config
        if event_order_path is not None and not event_order_path.is_absolute():
            config.config.event_order_config = (
                file_path.parent / event_order_path
            ).resolve()

        return config
