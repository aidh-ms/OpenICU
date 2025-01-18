from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pandera.typing import DataFrame

from open_icu.types.conf.concept import Concept, ConceptSource
from open_icu.types.conf.source import SourceConfig
from open_icu.types.fhir import FHIRFlattenSchema
from open_icu.types.fhir.utils import to_identifiers_str

F = TypeVar("F", bound=FHIRFlattenSchema)


class ConceptExtractor(ABC, Generic[F]):
    def __init__(self, subject_id: str, source: SourceConfig, concept: Concept, concept_source: ConceptSource) -> None:
        super().__init__()
        self._source = source
        self._concept = concept
        self._concept_source = concept_source

        self._concept_source.params["subject_id"] = subject_id

    def __call__(self) -> DataFrame[F] | None:
        return self.extract()

    @abstractmethod
    def extract(self) -> DataFrame[F] | None:
        raise NotImplementedError

    def _apply_identifier__coding(self, df: DataFrame) -> str:
        return to_identifiers_str(self._concept.identifiers)

    def _apply_subject__reference(self, df: DataFrame) -> str:
        return str(df["subject_id"])

    def _apply_subject__type(self, df: DataFrame) -> str:
        return self._concept_source.source
