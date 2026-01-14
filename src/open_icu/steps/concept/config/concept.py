

from pydantic import Field, computed_field

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
    unit: str = Field(..., description="Unit of measurement for the concept values.")

    @computed_field
    @property
    def identifier_tuple(self) -> tuple[str, ...]:
        return "concept", self.name, self.version
