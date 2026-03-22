"""Base configuration class with identifier generation and serialization.

This module defines the abstract base class for all configuration objects
in OpenICU, providing standardized naming, versioning, identifier generation,
and YAML serialization capabilities.
"""

from abc import ABCMeta
from pathlib import Path
from typing import ClassVar, Self
from uuid import NAMESPACE_DNS, UUID, uuid5

import yaml
from pydantic import BaseModel, Field, computed_field


class BaseConfig(BaseModel, metaclass=ABCMeta):
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
    __open_icu_config_type__: ClassVar[str] = "base"

    name: str = Field(..., description="Name of the configuration.")
    version: str = Field(..., description="Version of the configuration.")

    def __str__(self) -> str:
        return self.identifier

    @computed_field
    @property
    def config_type(self) -> str:
        """Get the configuration type.

        Returns:
            The configuration type string
        """
        return self.__open_icu_config_type__

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
        return self.__open_icu_config_type__, self.name, self.version

    @computed_field
    @property
    def uuid(self) -> UUID:
        """Generate a UUID based on the configuration identifier.

        Uses UUID5 with DNS namespace to create a deterministic UUID
        from the identifier string.

        Returns:
            A UUID uniquely identifying this configuration
        """
        return uuid5(NAMESPACE_DNS, self.identifier)

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
    def prefix(cls) -> str:
        """Get the prefix of the identifier.

        Returns:
            The prefix string (e.g., "openicu.config")
        """
        return cls.build_identifier((cls.__open_icu_config_type__,))

    @classmethod
    def ensure_prefix(cls, identifier: str) -> str:
        """Ensure the identifier has the correct prefix.

        If the identifier does not start with the expected prefix, it is
        prepended to ensure consistency.

        Args:
            identifier: The identifier string to check

        Returns:
            The identifier string with the correct prefix
        """
        prefix = cls.prefix()
        if not identifier.startswith(prefix):
            return f"{prefix}.{identifier}"
        return identifier

    @classmethod
    def load(cls, file_path: Path, **kwargs) -> Self:
        """Load configuration from a YAML file.

        Args:
            file_path: Path to the YAML configuration file
            **kwargs: Additional keyword arguments for configuration initialization

        Returns:
            Configuration instance populated from the YAML file

        Raises:
            FileNotFoundError: If file_path does not exist
            yaml.YAMLError: If YAML parsing fails
        """
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)

        for k, v in kwargs.items():
            if k not in data:
                data[k] = v

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


class BaseDatasetConfig(BaseConfig):
    """Base configuration class for dataset configurations.

    Inherits from BaseConfig and can be extended with dataset-specific
    attributes and methods in the future.
    """
    dataset: str = Field(..., description="Name of the dataset associated with this dataset configuration.")

    @classmethod
    def load(cls, file_path: Path, **kwargs) -> Self:
        """Load configuration from a YAML file.

        Args:
            file_path: Path to the YAML configuration file
            **kwargs: Additional keyword arguments for configuration initialization

        Returns:
            Configuration instance populated from the YAML file

        Raises:
            FileNotFoundError: If file_path does not exist
            yaml.YAMLError: If YAML parsing fails
        """
        *_, dataset, version, _, _ = file_path.parts
        name = file_path.stem

        return super().load(file_path, dataset=dataset, name=name, version=version)
