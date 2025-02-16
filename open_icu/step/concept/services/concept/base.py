from abc import ABCMeta, abstractmethod
from typing import Any, Generic

from dependency_injector.containers import Container
from dependency_injector.wiring import Provide, inject
from pandera.typing import DataFrame

from open_icu.data.proto import IDataFrameDatabaseExtractor
from open_icu.step.concept.conf import ConceptConfig, ConceptSourceConfig
from open_icu.step.concept.proto import FHIR_TYPE, IConceptService
from open_icu.type.fhir.utils import to_identifiers_str


class ConceptExtractor(IConceptService, Generic[FHIR_TYPE], metaclass=ABCMeta):
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

    def __init__(self, concept_source_config: ConceptSourceConfig, *args: Any, **kwargs: Any) -> None:
        self._concept_source_config = concept_source_config
        self._args = args
        self._kwargs = kwargs

    @abstractmethod
    def __call__(self, concept_config: ConceptConfig, *args: Any, **kwargs: Any) -> DataFrame[FHIR_TYPE] | None:
        """
        A shortcut to the `extract` method.

        Parameters
        ----------
        concept_config : ConceptConfig
            The concept configuration.
        args : Any
            The arguments to be passed to the extract method.
        kwargs : Any
            The keyword arguments to be passed to the extract method.

        Returns
        -------
        DataFrame[F] | None
            The extracted concept data.
        """
        raise NotImplementedError

    @inject
    def _get_data(
        self,
        concept_source_config: ConceptSourceConfig,
        container: Container = Provide["<container>"],
        *args: Any,
        **kwargs: Any,
    ) -> DataFrame | None:
        """
        A method to get the data from the source.

        Parameters
        ----------
        concept_source_config : ConceptSourceConfig
            The concept source configuration.
        container : Container
            The di container containing the database extractor.
        args : Any
            The arguments to be passed to the extract method.
        kwargs : Any
            The keyword arguments to be passed to the extract method.

        Returns
        -------
        DataFrame | None
            The extracted data.
        """
        df_extractor: IDataFrameDatabaseExtractor = getattr(container, f"db_{self._concept_source_config.source}")()
        kwargs = self._concept_source_config.kwargs.copy()
        query = kwargs.pop("query")

        return df_extractor.get_df(query, **kwargs)

    def _apply_identifier__coding(self, df: DataFrame, concept_config: ConceptConfig | None = None) -> str:
        """
        A method to apply to the concept DataFrame to add the identifier coding to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the identifier coding to.
        concept_config : ConceptConfig, optional
            The concept configuration.

        Returns
        -------
        str
            The identifier coding.
        """
        assert concept_config is not None
        return to_identifiers_str(concept_config.identifiers)

    def _apply_subject__reference(self, df: DataFrame, concept_config: ConceptConfig | None = None) -> str:
        """
        A method to apply to the concept DataFrame to add the subject reference to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the subject reference to.
        concept_config : ConceptConfig, optional
            The concept configuration.

        Returns
        -------
        str
            The subject reference.
        """
        return str(df["subject_id"])

    def _apply_subject__type(self, df: DataFrame, concept_config: ConceptConfig | None = None) -> str:
        """
        A method to apply to the concept DataFrame to add the subject type to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to apply the subject type to.
        concept_config : ConceptConfig, optional
            The concept configuration.

        Returns
        -------
        str
            The subject type.
        """
        return self._concept_source_config.source
