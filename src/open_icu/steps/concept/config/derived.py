from typing import Literal

from pydantic import BaseModel, Field, computed_field

from open_icu.config.base import BaseDatasetConfig


class BaseConceptTable(BaseModel):
    concept: str = Field(..., description="The identifier of the concept this table represents.")
    pre_callbacks: list[str] = Field(
        default_factory=list, description="The list of callback configurations for the table."
    )
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

    @computed_field
    @property
    def join_params(self) -> dict[str, list[str]]:
        params = {}
        if self.both_on:
            params["on"] = self.both_on
        if self.left_on:
            params["left_on"] = self.left_on
        if self.right_on:
            params["right_on"] = self.right_on

        return params

class ConceptTable(BaseConceptTable):
    pass


class MEDSConceptTable(BaseModel):
    subject_id: str | None = Field("subject_id", description="The default subject identifier column name.")
    time: str | None = Field("time", description="The default timestamp column name.")
    code: list[str] | None = Field(None, description="The default code column name.")
    numeric_value: str | None = Field(None, description="The default numeric value column name.")
    text_value: str | None = Field(None, description="The default text value column name.")
    extension: dict[str, str] | None = Field(None, description="The default extension column name mapping.")


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
    event: MEDSConceptTable = Field(
        default_factory=..., description="The configuration for the MEDS event concept table to be derived (if applicable)."
    )
    filters: list[str] = Field(
        default_factory=list, description="The list of filter configurations for the derived concept."
    )
