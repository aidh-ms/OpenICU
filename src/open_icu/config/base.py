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
        return self.build_identifier(self.identifier_tuple)

    @computed_field
    @property
    def identifier_tuple(self) -> tuple[str, ...]:
        return self.__class__.__name__, self.version, self.name

    @computed_field
    @property
    def uuid(self) -> UUID:
        """Generate a UUID based on the name and version of the config."""
        return uuid5(NAMESPACE_DNS, self.identifier)  # type: ignore[unresolved-attribute]

    @classmethod
    def build_identifier(cls, t: tuple[str, ...]) -> str:
        id = ".".join(t).lower()
        return f"openicu.config.{id}"

    @classmethod
    def load(cls, file_path: Path) -> Self:
        """Load configuration from a dictionary."""
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)

        return cls(**data)

    def save(self, path: Path) -> None:
        """Save configuration to a YAML file."""

        path = Path(path, *self.identifier_tuple).with_suffix(".yml")
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            yaml.safe_dump(self.model_dump(mode="json", exclude_computed_fields=True), f)
