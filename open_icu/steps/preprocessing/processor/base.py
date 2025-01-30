from abc import ABC, abstractmethod

from open_icu.types.base import SubjectData


class Preprocessor(ABC):
    """
    A base class for preprocessors.

    Parameters
    ----------
    concepts : list[str]
        The list of concepts to preprocess.
    """

    def __init__(self, concepts: list[str]) -> None:
        super().__init__()

        self.concepts = concepts

    def __call__(self, subject_data: SubjectData) -> SubjectData:
        """
        A Shorthand for the process method.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to preprocess.

        Returns
        -------
        SubjectData
            The preprocessed subject data.
        """
        return self.process(subject_data)

    @abstractmethod
    def process(self, subject_data: SubjectData) -> SubjectData:
        """
        An abstract method to preprocess the subject data.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to preprocess.

        Returns
        -------
        SubjectData
            The preprocessed subject data.
        """
        raise NotImplementedError
