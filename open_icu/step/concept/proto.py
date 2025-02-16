from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

from pandera.typing import DataFrame

from open_icu.type.fhir import FHIRFlattenSchema

if TYPE_CHECKING:
    from open_icu.step.concept.conf import ConceptConfig, ConceptSourceConfig

FHIR_TYPE = TypeVar("FHIR_TYPE", bound=FHIRFlattenSchema)


class IConceptService(Protocol, Generic[FHIR_TYPE]):
    """
    A protocol for the sampler service.
    """

    def __init__(self, concept_source_config: ConceptSourceConfig, *args: Any, **kwargs: Any) -> None:
        ...

    def __call__(
        self, concept_config: ConceptConfig, subject_id: str, *args: Any, **kwargs: Any
    ) -> DataFrame[FHIR_TYPE] | None:
        ...

    def _get_data(self, subject_id: str, *args: Any, **kwargs: Any) -> DataFrame[Any]:
        ...
