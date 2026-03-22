from typing import Literal

from pydantic import BaseModel, Field

from open_icu.config.base import BaseDatasetConfig


class BaseConceptTable(BaseModel):
    concept: str = Field(..., description="The identifier of the concept this table represents.")
    callbacks: list[str] = Field(
        default_factory=list, description="The list of callback configurations for the table."
    )
    post_callbacks: list[str] = Field(
        default_factory=list, description="The list of callback configurations for the table."
    )
    columns: list[str] = Field(
        default_factory=list, description="The list of column names to include in the concept table."
    )


class JoinConceptTable(BaseConceptTable):
    type: Literal["join"] = Field(
        "join", description="Type of concept table: 'join' or 'aggregate'."
    )
    both_on: list[str] = Field(
        default_factory=lambda: ["subject_id", "time"],
        description="List of columns to be used for joining table on both sides.",
    )
    left_on: list[str] = Field(
        default_factory=list,
        description="List of columns to be used for joining table on the left side.",
    )
    right_on: list[str] = Field(
        default_factory=list,
        description="List of columns to be used for joining table on the right side.",
    )
    how: str = Field(
        "outer",
        description="Type of join to be performed (e.g. inner, left, right, outer).",
    )

class ConceptTable(BaseConceptTable):
    pass


class DerivedDatasetConceptConfig(BaseDatasetConfig):
    """Configuration for a derived dataset-specific concept.

    Inherits from BaseDatasetConfig and adds attributes specific to derived concepts.
    """
    __open_icu_config_type__ = "concept"

    type: Literal["derived"] = Field(
        "derived", description="Type of concept: 'base', 'derived', or 'complex'."
    )
    table: ConceptTable = Field(..., description="The configuration for the concept table to be derived.")
    join: list[JoinConceptTable] = Field(
        default_factory=list, description="The list of join configurations for the derived concept."
    )
