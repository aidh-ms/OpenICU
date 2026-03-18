from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import Field, ValidationError, computed_field, model_validator

from open_icu.config.base import BaseConfig, BaseDatasetConfig
from open_icu.logging import logger
from open_icu.steps.concept.config.mapping import MappingConfig


class DatasetConceptConfig(BaseDatasetConfig):
    """Configuration for a dataset-specific concept.

    Inherits from ConceptConfig and adds dataset-specific attributes if needed.
    """
    __open_icu_config_type__ = "concept"

    mappings: list[MappingConfig] = Field(default_factory=list, description="List of concept mappings.")

    @model_validator(mode="after")
    def inject_dataset_into_mappings(self) -> "DatasetConceptConfig":
        for mapping in self.mappings:
            mapping.pattern.dataset = self.dataset
            mapping.pattern.version = self.version
        return self


class ConceptConfig(BaseConfig):
    """Configuration for a concept table.

    Attributes:
        name: Human-readable name of the configuration
        version: Version string for the configuration
        identifier: Computed hierarchical identifier (e.g., "openicu.config.classname.version.name")
        identifier_tuple: Tuple of (class_name, version, name)
        uuid: UUID generated from the identifier
        unit: Unit of measurement for the concept values
        type: Type of concept: 'base', 'dependent', or 'complex'
        extension_columns: Dictionary of extension columns to include in the concept table
        mappings: List of MappingConfig objects defining how to extract concept data
    """
    __open_icu_config_type__ = "concept"

    unit: str = Field(..., description="Unit of measurement for the concept values.")
    type: Literal["base", "dependent", "complex"] = Field(
        "base", description="Type of concept: 'base', 'dependent', or 'complex'."
    )

    extension_columns: dict[str, str] = Field(
        default_factory=dict,
        description="Dictionary of extension columns to include in the concept table.",
    )

    dataset_concepts: list[DatasetConceptConfig] = Field(
        default_factory=list,
        description="List of dataset-specific concepts that this concept depends on (for dependent concepts).",
    )

    @computed_field
    @property
    def code(self) -> str:
        """Return the code column name based on concept type."""
        return f"{self.name}//{self.unit}"

    @classmethod
    def load(cls, file_path: Path, dataset_paths: list[Path] | None = None, **kwargs) -> Self:
        """Load configuration from a YAML file.

        Args:
            file_path: Path to the YAML configuration file
            dataset_paths: List of paths to dataset directories
            **kwargs: Additional keyword arguments for configuration initialization

        Returns:
            Configuration instance populated from the YAML file

        Raises:
            FileNotFoundError: If file_path does not exist
            yaml.YAMLError: If YAML parsing fails
        """
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)

        name = data.get("name")
        paths = dataset_paths or []
        for path in paths:
            if not (path / f"{name}.yml").exists():
                continue

            try:
                dataset_concept = DatasetConceptConfig.load(path / f"{name}.yml")
                data.setdefault("dataset_concepts", []).append(dataset_concept)
            except ValidationError:
                logger.warning("failed to load dataset concept config for %s from %s", name, path)

        for k, v in kwargs.items():
            if k not in data:
                data[k] = v

        return cls(**data)
