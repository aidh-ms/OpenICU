"""Configuration models for the persistence step."""

from pathlib import Path

from pydantic import BaseModel, Field

from open_icu.steps.base.config import BaseStepConfig


class TableConfig(BaseModel):
    """Configuration for a source table to persist."""

    name: str = Field(
        ...,
        description="Name used for the output Parquet file.",
    )
    path: Path = Field(
        ...,
        description="Path to the source CSV file relative to the dataset directory.",
    )


class DatasetConfig(BaseModel):
    """Configuration for a source dataset whose tables should be persisted."""

    name: str = Field(
        ...,
        description="Name of the source dataset.",
    )
    version: str = Field(
        ...,
        description="Version of the source dataset.",
    )
    path: Path = Field(
        ...,
        description="Path to the source dataset directory.",
    )
    tables: list[TableConfig] = Field(
        default_factory=list,
        description="Source CSV tables to persist.",
    )


class CustomConfig(BaseModel):
    """Configuration specific to the persistence step."""

    data: list[DatasetConfig] = Field(
        default_factory=list,
        description="Source datasets and tables to persist.",
    )


class PersistenceStepConfig(BaseStepConfig[CustomConfig]):
    """Complete configuration for the persistence step."""

    pass
