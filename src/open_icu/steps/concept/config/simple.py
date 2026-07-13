from typing import Literal

from pydantic import BaseModel, Field, computed_field, model_validator

from open_icu.config.base import BaseDatasetConfig


class MappingColumnConfig(BaseModel):
    """Configuration for a concept mapping.

    Attributes:
        numeric_value: Column name for numeric values
        text_value: Column name for text values
    """
    numeric_value: str | None = Field(None, description="Column name for numeric values.")
    text_value: str | None = Field(None, description="Column name for text values.")


class MappingPatternConfig(BaseModel):
    """Configuration for concept mapping patterns.

    Attributes:
        dataset: Dataset name to match.
        version: Dataset version to match.
        table: Table name to match.
        event: Event name to match.
        code: Code value to match.
        regex: Regular expression pattern to match.
    """
    dataset: str = Field(..., description="Dataset name to match.")
    version: str | None = Field(None, description="Dataset version to match.")
    table: str = Field(..., description="Table name to match.")
    event: str = Field(..., description="Event name to match.")
    code: str = Field(..., description="Code value to match.")



class MappingConfig(BaseModel):
    """Configuration for a concept mapping.

    Attributes:
        pattern: Pattern configuration for concept mapping.
        columns: Column configuration for concept mapping.
        filters: The list of filter configurations for the mapping.
    """
    pattern: MappingPatternConfig = Field(..., description="Pattern configuration for concept mapping.")
    columns: MappingColumnConfig = Field(..., description="Column configuration for concept mapping.")

    @computed_field
    @property
    def regex(self) -> str:
        """Returns the regex pattern if defined in the mapping pattern."""
        return self.pattern.code or "(.+?)"

    filters: list[str] = Field(
        default_factory=list, description="The list of filter configurations for the mapping."
    )


class SimpleDatasetConceptConfig(BaseDatasetConfig):
    """Configuration for a dataset-specific concept.

    Inherits from BaseDatasetConfig and adds dataset-specific attributes if needed.
    """
    __open_icu_config_type__ = "concept"

    mappings: list[MappingConfig] = Field(default_factory=list, description="List of concept mappings.")
    type: Literal["simple"] = Field(
        "simple", description="Type of concept: 'base', 'derived', or 'complex'."
    )

    @model_validator(mode="after")
    def inject_dataset_into_mappings(self) -> "SimpleDatasetConceptConfig":
        for mapping in self.mappings:
            mapping.pattern.dataset = self.dataset
            mapping.pattern.version = self.version
        return self
