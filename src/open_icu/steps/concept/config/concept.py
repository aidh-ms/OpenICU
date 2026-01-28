from typing import Literal

from pydantic import Field, computed_field

from open_icu.config.base import BaseConfig
from open_icu.steps.concept.config.mapping import MappingConfig


class ConceptConfig(BaseConfig):
    """Configuration for a concept table.

    Attributes:
        name: Human-readable name of the configuration
        version: Version string for the configuration
        identifier: Computed hierarchical identifier (e.g., "openicu.config.classname.version.name")
        identifier_tuple: Tuple of (class_name, version, name)
        uuid: UUID generated from the identifier
        unit: Unit of measurement for the concept values
        type: Type of concept: 'base' or 'derived'
        extension_columns: Dictionary of extension columns to include in the concept table
        mappings: List of MappingConfig objects defining how to extract concept data
    """
    __open_icu_config_type__ = "concept"

    unit: str = Field(..., description="Unit of measurement for the concept values.")
    type: Literal["base", "derived"] = Field(
        "base", description="Type of concept: 'base' or 'derived'."
    )

    extension_columns: dict[str, str] = Field(
        default_factory=dict,
        description="Dictionary of extension columns to include in the concept table.",
    )

    mappings: list[MappingConfig] = Field(default_factory=list, description="List of concept mappings.")

    @computed_field
    @property
    def code(self) -> str:
        """Return the code column name based on concept type."""
        return f"{self.name}//{self.unit}"
