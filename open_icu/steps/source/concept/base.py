from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pandera.typing import DataFrame

from open_icu.types.conf.concept import ConceptConfig, ConceptSource
from open_icu.types.conf.source import SourceConfig
from open_icu.types.fhir import FHIRFlattenSchema
from open_icu.types.fhir.utils import to_identifiers_str

F = TypeVar("F", bound=FHIRFlattenSchema)


class ConceptExtractor(ABC, Generic[F]):
    """
    A base class for extracting data from a source based on a concept.

    Parameters
    ----------
    subject_id : str
        The subject ID to extract data for.
    source : SourceConfig
        The source configuration.
    concept : ConceptConfig
        The concept configuration.
    concept_source : ConceptSource
        The concept source configuration.
    """

    def __init__(
        self, subject_id: str, source: SourceConfig, concept: ConceptConfig, concept_source: ConceptSource
    ) -> None:
        super().__init__()
        self._source = source
        self._concept = concept
        self._concept_source = concept_source

        self._concept_source.params["subject_id"] = subject_id

    def __call__(self) -> DataFrame[F] | None:
        """
        A shortcut to the `extract` method.

        Returns
        -------
        DataFrame[F] | None
            The extracted concept data.
        """
        return self.extract()

    @abstractmethod
    def extract(self) -> DataFrame[F] | None:
        """
        Extract data from the source based on the concept.

        Returns
        -------
        DataFrame[F] | None
            The extracted concept data.
        """
        raise NotImplementedError

    def _apply_identifier__coding(self, df: DataFrame) -> str:
        """
        A method to apply to the concept DataFrame to add the identifier coding to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the identifier coding to.

        Returns
        -------
        str
            The identifier coding.
        """
        return to_identifiers_str(self._concept.identifiers)

    def _apply_subject__reference(self, df: DataFrame) -> str:
        """
        A method to apply to the concept DataFrame to add the subject reference to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the subject reference to.

        Returns
        -------
        str
            The subject reference.
        """
        return str(df["subject_id"])

    def _apply_subject__type(self, df: DataFrame) -> str:
        """
        A method to apply to the concept DataFrame to add the subject type to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the subject type to.

        Returns
        -------
        str
            The subject type.
        """
        return self._concept_source.source
