from abc import ABCMeta
from pathlib import Path
from typing import Self
from uuid import NAMESPACE_DNS, UUID, uuid5

import yaml
from pydantic import BaseModel, Field, computed_field


class BaseConfig(BaseModel, metaclass=ABCMeta):
    name: str = Field(..., description="Name of the configuration.")
    version: str = Field(..., description="Version of the configuration.")

    @computed_field
    @property
    def identifier(self) -> str:
        """Generate an OID based on the name and version of the config."""
        return self.build_identifier(self.name, self.version)

    @computed_field
    @property
    def uuid(self) -> UUID:
        """Generate a UUID based on the name and version of the config."""
        return uuid5(NAMESPACE_DNS, self.identifier)  # type: ignore[unresolved-attribute]

    @classmethod
    def build_identifier(cls, name: str, version: str) -> str:
        return f"openicu.config.{cls.__name__.lower()}.{name.lower()}.{version.lower()}"

    @classmethod
    def load(cls, file_path: Path) -> Self:
        """Load configuration from a dictionary."""
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)

        return cls(**data)

    def save(self, file_path: Path) -> None:
        """Save configuration to a YAML file."""
        with open(file_path, "w") as f:
            yaml.safe_dump(self.model_dump(mode="json"), f)
