from pathlib import Path
from typing import Annotated, Self

import yaml
from pydantic import Field, TypeAdapter, ValidationError, computed_field

from open_icu.config.base import BaseConfig
from open_icu.logging import logger
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.derived import DerivedDatasetConceptConfig
from open_icu.steps.concept.config.simple import SimpleDatasetConceptConfig

DatasetConceptConfigUnion = Annotated[
    SimpleDatasetConceptConfig
    | DerivedDatasetConceptConfig
    | ComplexDatasetConceptConfig,
    Field(discriminator="type")
]


class ConceptConfig(BaseConfig):
    """Configuration for a concept table.

    Attributes:
        name: Human-readable name of the configuration
        version: Version string for the configuration
        identifier: Computed hierarchical identifier (e.g., "openicu.config.classname.version.name")
        identifier_tuple: Tuple of (class_name, version, name)
        uuid: UUID generated from the identifier
        unit: Unit of measurement for the concept values
        extension_columns: Dictionary of extension columns to include in the concept table
        dataset_concepts: List of DatasetConceptConfig objects defining how to extract concept data per dataset
    """
    __open_icu_config_type__ = "concept"

    unit: str = Field(..., description="Unit of measurement for the concept values.")
    extension_columns: dict[str, str] = Field(
        default_factory=dict,
        description="Dictionary of extension columns to include in the concept table.",
    )

    dataset_concepts: list[DatasetConceptConfigUnion] = Field(
        default_factory=list,
        description="List of dataset-specific concepts that this concept depends on (for dependent concepts).",
    )

    @computed_field
    @property
    def code(self) -> str:
        """Return the code column name based on concept type."""
        return "{self.name}" + ("//{self.unit}" if self.unit is not None else "")

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
            sub_file_path = path / f"{name}.yml"
            if not sub_file_path.exists():
                continue

            adapter = TypeAdapter(DatasetConceptConfigUnion)
            try:
                with open(sub_file_path, "r") as f:
                    sub_data = yaml.safe_load(f)

                *_, dataset, version, _, _ = sub_file_path.parts
                sub_data.update({
                    "dataset": dataset,
                    "version": version,
                    "name": sub_file_path.stem,
                })

                dataset_concept = adapter.validate_python(sub_data)
                data.setdefault("dataset_concepts", []).append(dataset_concept)
            except ValidationError:
                logger.warning("failed to load dataset concept config for %s from %s", name, sub_file_path)

        for k, v in kwargs.items():
            if k not in data:
                data[k] = v

        return cls(**data)

    def get_dataset_concept(self, dataset_name: str) -> DatasetConceptConfigUnion | None:
        """Get the dataset-specific concept configuration for a given dataset name.

        Args:
            dataset_name: Name of the dataset to retrieve the concept configuration for
        Returns:
            The DatasetConceptConfig instance for the specified dataset, or None if not found
        """
        for dataset_concept in self.dataset_concepts:
            if dataset_concept.dataset == dataset_name:
                return dataset_concept
        return None
