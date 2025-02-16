from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

from pandera.typing import DataFrame

if TYPE_CHECKING:
    from open_icu.step.concept.conf import ConceptConfig, ConceptSourceConfig
    from open_icu.type.fhir import FHIRFlattenSchema

FHIR_TYPE = TypeVar("FHIR_TYPE", bound=FHIRFlattenSchema)


class IConceptService(Protocol, Generic[FHIR_TYPE]):
    """
    A protocol for the sampler service.
    """

    def __init__(self, concept_source_config: ConceptSourceConfig, *args: Any, **kwargs: Any) -> None:
        ...

    def __call__(self, concept_config: ConceptConfig, *args: Any, **kwargs: Any) -> DataFrame[FHIR_TYPE] | None:
        ...

    def _get_data(self, *args: Any, **kwargs: Any) -> DataFrame[Any]:
        ...
