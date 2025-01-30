from abc import ABC, abstractmethod

from open_icu.types.base import SubjectData


class CohortFilter(ABC):
    """
    A base class for filtering subjects based on their data.

    Parameters
    ----------
    concepts : list[str]
        The concepts to filter on.
    """

    def __init__(self, concepts: list[str]) -> None:
        super().__init__()

        self.concepts = concepts

    def __call__(self, subject_data: SubjectData) -> bool:
        """
        A convenience method to call the filter method.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to filter.

        Returns
        -------
        bool
            Whether the subject should be included or not.
        """

        return self.filter(subject_data)

    @abstractmethod
    def filter(self, subject_data: SubjectData) -> bool:
        """
        An abstract method to filter out subjects based on their data.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to filter.

        Returns
        -------
        bool
            Whether the subject should be included or not.
        """
        raise NotImplementedError
