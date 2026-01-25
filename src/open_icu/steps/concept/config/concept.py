from typing import Literal

from pydantic import Field

from open_icu.config.base import BaseConfig


class ConceptConfig(BaseConfig):
    """Configuration for a concept table.

    Attributes:
        name: Human-readable name of the configuration
        version: Version string for the configuration
        identifier: Computed hierarchical identifier (e.g., "openicu.config.classname.version.name")
        identifier_tuple: Tuple of (class_name, version, name)
        uuid: UUID generated from the identifier
        unit: Unit of measurement for the concept values
    """
    __open_icu_config_type__ = "concept"

    unit: str = Field(..., description="Unit of measurement for the concept values.")
    type: Literal["base", "derived"] = Field(
        "base", description="Type of concept: 'base' or 'derived'."
    )

    extension_columns: list[str] = Field(
        default_factory=list,
        description="List of extension columns to include in the concept table.",
    )
