from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, Protocol

if TYPE_CHECKING:
    from open_icu.step.concept.conf import ConceptConfig, ConceptSourceConfig
    from open_icu.type.subject import SubjectData


class IConceptService(Protocol):
    """
    A protocol for the sampler service.
    """

    def __init__(self, concept_source_config: ConceptSourceConfig, *args: Any, **kwargs: Any) -> None:
        ...

    def __call__(self, concept_config: ConceptConfig, *args: Any, **kwargs: Any) -> Iterator[SubjectData]:
        ...
