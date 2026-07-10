"""Configuration models for the sharding step."""

from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig


class CustomConfig(BaseModel):
    """Configuration specific to the sharding step.

    The sharding step reads the output of a preceding concept step and writes
    long-format Parquet shards grouped by subject. Empty dataset or concept
    lists mean "include all".
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


class ShardingStepConfig(BaseStepConfig[CustomConfig]):
    """Complete configuration for the sharding step."""

    pass
