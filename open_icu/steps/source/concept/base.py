from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pandera.typing import DataFrame

from open_icu.types.conf.concept import Concept, ConceptSource
from open_icu.types.conf.source import SourceConfig
from open_icu.types.fhir import (
    FHIRSchema,
)

F = TypeVar("F", bound=FHIRSchema)


class ConceptExtractor(ABC, Generic[F]):
    def __init__(self, subject_id: str, source: SourceConfig, concept: Concept, concept_source: ConceptSource) -> None:
        self._source = source
        self._concept = concept
        self._concept_source = concept_source

        self._concept_source.params["subject_id"] = subject_id

    def __call__(self) -> DataFrame[F] | None:
        return self.extract()

    @abstractmethod
    def extract(self) -> DataFrame[F] | None:
        raise NotImplementedError
