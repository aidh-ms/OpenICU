"""Base configuration class with identifier generation and serialization.

This module defines the abstract base class for all configuration objects
in OpenICU, providing standardized naming, versioning, identifier generation,
and YAML serialization capabilities.
"""

from abc import ABCMeta
from pathlib import Path
from typing import Self, Any
from uuid import NAMESPACE_DNS, UUID, uuid5

import yaml
from pydantic import BaseModel, Field, computed_field

from open_icu.representation import Representable


class BaseConfig(BaseModel, Representable, metaclass=ABCMeta):
    """Abstract base class for configuration objects.

    Provides automatic identifier and UUID generation based on name and version,
    along with methods for loading from and saving to YAML files. All
    configuration classes should inherit from this base class.

    Attributes:
        name: Human-readable name of the configuration
        version: Version string for the configuration
        identifier: Computed hierarchical identifier (e.g., "openicu.config.classname.version.name")
        identifier_tuple: Tuple of (class_name, version, name)
        uuid: UUID generated from the identifier
    """
    name: str = Field(..., description="Name of the configuration.")
    version: str = Field(..., description="Version of the configuration.")

    @computed_field
    @property
    def identifier(self) -> str:
        """Generate a hierarchical identifier string.

        Creates a unique identifier in the format:
        "openicu.config.{classname}.{version}.{name}" (all lowercase)

        Returns:
            A dot-separated hierarchical identifier string
        """
        return self.build_identifier(self.identifier_tuple)

    @computed_field
    @property
    def identifier_tuple(self) -> tuple[str, ...]:
        """Get the components used to build the identifier.

        Returns:
            Tuple of (class_name, version, name)
        """
        return self.__class__.__name__, self.version, self.name

    @computed_field
    @property
    def uuid(self) -> UUID:
        """Generate a UUID based on the configuration identifier.

        Uses UUID5 with DNS namespace to create a deterministic UUID
        from the identifier string.

        Returns:
            A UUID uniquely identifying this configuration
        """
        return uuid5(NAMESPACE_DNS, self.identifier)  # type: ignore[unresolved-attribute]

    @classmethod
    def build_identifier(cls, t: tuple[str, ...]) -> str:
        """Build a hierarchical identifier from a tuple of components.

        Args:
            t: Tuple of identifier components

        Returns:
            Dot-separated identifier string with "openicu.config." prefix
        """
        id = ".".join(t).lower()
        return f"openicu.config.{id}"

    @classmethod
    def load(cls, file_path: Path) -> Self:
        """Load configuration from a YAML file.

        Args:
            file_path: Path to the YAML configuration file

        Returns:
            Configuration instance populated from the YAML file

        Raises:
            FileNotFoundError: If file_path does not exist
            yaml.YAMLError: If YAML parsing fails
        """
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)

        return cls(**data)

    def save(self, path: Path) -> None:
        """Save configuration to a YAML file.

        Creates a hierarchical directory structure under path using the
        identifier components, and saves the configuration as a YAML file.
        Computed fields are excluded from the saved output.

        Args:
            path: Base directory path for saving the configuration
        """
        path = Path(path, *self.identifier_tuple).with_suffix(".yml")
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            yaml.safe_dump(self.model_dump(mode="json", exclude_computed_fields=True), f)

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "version": self.version, "identifier": self.identifier, "uuid": self.uuid}

    def summary(self) -> dict[str, Any]:
        return {"name": self.name, "version": self.version}