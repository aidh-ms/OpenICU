from pathlib import Path
from typing import Annotated, Self

import yaml
from pydantic import BaseModel, Field, PrivateAttr, TypeAdapter, ValidationError, computed_field, model_validator

from open_icu.config.base import BaseConfig
from open_icu.config.inheritance import has_extends, resolve_effective_configs
from open_icu.logging import logger
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.derived import DerivedDatasetConceptConfig
from open_icu.steps.concept.config.simple import SimpleDatasetConceptConfig

DatasetConceptConfigUnion = Annotated[
    SimpleDatasetConceptConfig | DerivedDatasetConceptConfig | ComplexDatasetConceptConfig, Field(discriminator="type")
]


class ConceptLimits(BaseModel):
    """Configuration for concept limits.

    Attributes:
        min: Minimum value for the concept.
        max: Maximum value for the concept.
    """

    min: float | None = Field(None, description="Minimum value for the concept.")
    max: float | None = Field(None, description="Maximum value for the concept.")


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
        limits: Configuration for concept limits
        dataset_concepts: List of DatasetConceptConfig objects defining how to extract concept data per dataset
    """

    __open_icu_config_type__ = "concept"

    unit: str = Field(..., description="Unit of measurement for the concept values.")
    limits: ConceptLimits = Field(default_factory=ConceptLimits)
    extension_columns: dict[str, str] = Field(
        default_factory=dict,
        description="Dictionary of extension columns to include in the concept table.",
    )

    dataset_concepts: list[DatasetConceptConfigUnion] = Field(
        default_factory=list,
        description="List of dataset-specific concepts that this concept depends on (for dependent concepts).",
    )

    default: dict | None = Field(
        None,
        description=(
            "Dataset-agnostic definition, applied to any dataset that provides no mapping of its own. "
            "Only 'derived' and 'complex' concepts may declare one: a 'simple' mapping is a set of "
            "source-code patterns and is irreducibly dataset-specific. A dataset opts out by shipping a "
            "mapping containing 'deleted: true'."
        ),
    )

    #: (dataset, version) pairs whose mapping is a `deleted: true` tombstone
    _suppressed: set[tuple[str, str]] = PrivateAttr(default_factory=set)
    #: per-(dataset, version) configs materialised from `default`
    _materialised_defaults: dict[tuple[str, str], DatasetConceptConfigUnion] = PrivateAttr(default_factory=dict)

    @computed_field
    @property
    def code(self) -> str:
        """Return the code column name based on concept type."""
        if self.unit is None:
            return self.name
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
        suppressed: set[tuple[str, str]] = set()
        adapter = TypeAdapter(DatasetConceptConfigUnion)
        for path in paths:
            if has_extends(path):
                # Resolve the dataset's inheritance chain; the mapping may be
                # inherited from (or merged with) a base version's config.
                sub_data = resolve_effective_configs(path).get(str(name))
                if sub_data is None:
                    continue
            else:
                sub_file_path = path / f"{name}.yml"
                if not sub_file_path.exists():
                    continue
                with open(sub_file_path, "r") as f:
                    sub_data = yaml.safe_load(f)

            dataset, version = path.parent.parent.name, path.parent.name

            # A tombstone is not a mapping: it says this dataset should not have
            # the concept at all, which also means no `default` may stand in.
            if isinstance(sub_data, dict) and sub_data.get("deleted") is True:
                suppressed.add((dataset, version))
                continue

            try:
                # Identity always comes from the dataset directory itself,
                # never from where an inherited file physically lives.
                sub_data.update(
                    {
                        "dataset": dataset,
                        "version": version,
                        "name": name,
                    }
                )

                dataset_concept = adapter.validate_python(sub_data)
                data.setdefault("dataset_concepts", []).append(dataset_concept)
            except ValidationError:
                logger.warning("failed to load dataset concept config for %s from %s", name, path)

        for k, v in kwargs.items():
            if k not in data:
                data[k] = v

        config = cls(**data)
        config._suppressed = suppressed
        return config

    def get_dataset_concept(self, dataset_name: str, version: str) -> DatasetConceptConfigUnion | None:
        """Get the dataset-specific concept configuration for a given dataset name.

        Args:
            dataset_name: Name of the dataset to retrieve the concept configuration for
            version: Version of the dataset
        Returns:
            The DatasetConceptConfig instance for the specified dataset, or None if not found
        """
        for dataset_concept in self.dataset_concepts:
            if dataset_concept.dataset == dataset_name and dataset_concept.version == version:
                return dataset_concept
        return self.default_for(dataset_name, version)

    def has_dataset_mapping(self, dataset_name: str, version: str) -> bool:
        """Whether this dataset defines the concept itself, rather than inheriting the default.
        """
        return any(dc.dataset == dataset_name and dc.version == version for dc in self.dataset_concepts)

    def default_for(self, dataset_name: str, version: str) -> DatasetConceptConfigUnion | None:
        """Materialise this concept's dataset-agnostic ``default`` for one dataset.

        Returns ``None`` when the concept declares no default, or when the
        dataset tombstoned the concept with ``deleted: true``. The result is
        cached, so the identity a transformer sees is stable across calls.
        """
        if self.default is None or (dataset_name, version) in self._suppressed:
            return None

        key = (dataset_name, version)
        if key not in self._materialised_defaults:
            data = {**self.default, "dataset": dataset_name, "version": version, "name": self.name}
            config = TypeAdapter(DatasetConceptConfigUnion).validate_python(data)
            if isinstance(config, ComplexDatasetConceptConfig):
                config._parent_concept = self
            self._materialised_defaults[key] = config

        return self._materialised_defaults[key]

    @model_validator(mode="after")
    def _link_complex_concepts_to_parent(self) -> "ConceptConfig":
        for dc in self.dataset_concepts:
            if isinstance(dc, ComplexDatasetConceptConfig):
                dc._parent_concept = self
        return self

    @model_validator(mode="after")
    def _validate_default(self) -> "ConceptConfig":
        """Reject a malformed ``default`` here rather than at first use.

        Validation needs a dataset and version, which a default by definition
        lacks, so it is checked against placeholders and the result discarded.
        """
        if self.default is None:
            return self

        declared = self.default.get("type")
        if declared not in ("derived", "complex"):
            raise ValueError(
                f"concept {self.name!r}: 'default' must be a 'derived' or 'complex' definition, got {declared!r}. "
                "A 'simple' mapping matches dataset-specific source codes and cannot be shared across datasets."
            )

        try:
            TypeAdapter(DatasetConceptConfigUnion).validate_python(
                {**self.default, "dataset": "__default__", "version": self.version, "name": self.name}
            )
        except ValidationError as exc:
            raise ValueError(f"concept {self.name!r}: invalid 'default' definition: {exc}") from exc

        return self
