from typing import TYPE_CHECKING, Literal, Protocol, cast

from pydantic import ConfigDict, Field, computed_field

from open_icu.config.base import BaseDatasetConfig
from open_icu.utils.importer import import_callable

if TYPE_CHECKING:
    from open_icu.steps.concept.config.concept import ConceptConfig
    from open_icu.storage.project import OpenICUProject


class ConceptTransformerProtocol(Protocol):

    def __init__(self, concept: "ConceptConfig", complex_config: "ComplexDatasetConceptConfig", **kwargs):
        ...
    def __call__(self, project: "OpenICUProject") -> None:
        ...


class ComplexDatasetConceptConfig(BaseDatasetConfig):
    """Configuration for a complex dataset-specific concept.

    Inherits from BaseDatasetConfig and adds attributes specific to complex concepts.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    __open_icu_config_type__ = "concept"

    type: Literal["complex"] = Field(
        "complex", description="Type of concept: 'base', 'derived', or 'complex'."
    )
    concept_transformer: str = Field(..., description="The name of the concept transformer function to apply to this concept (python dotted path).")
    kwargs: dict = Field(default_factory=dict, description="Additional keyword arguments to pass to the concept transformer function.")
    concepts: list[str] = Field(default_factory=list, description="The list of concept identifiers that this complex concept depends on.")

    @computed_field
    @property
    def fn(self) -> ConceptTransformerProtocol:
        """Dynamically import and return the concept transformer function based on the provided dotted path."""

        transformer = cast(
            type[ConceptTransformerProtocol],
            import_callable(self.concept_transformer)
        )
        return transformer(
            self.__class__.__bases__[0],
            self,
            **self.kwargs
        )

    @computed_field
    @property
    def dependencies(self) -> set[str]:
        """Get the set of concept dependencies for this derived concept.

        Returns:
            A set of concept identifiers that this derived concept depends on.
        """
        from open_icu.steps.concept.config.concept import ConceptConfig  # Avoid circular import

        deps = {
            ConceptConfig.ensure_prefix(concept)
            for concept in self.concepts
        }
        return deps
