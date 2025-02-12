from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Iterator

from open_icu.step.proto import StepProto
from open_icu.type.subject import SubjectData

if TYPE_CHECKING:
    from open_icu.step.exception import StepValidationError  # noqa


class BaseStep(ABC):
    """
    A base class for all steps in the pipeline.
    It follows the pattern of a pipe and filter architecture.

    The class is generic and expects a configuration class to be passed in.

    Parameters
    ----------
    fail_silently : bool, default: False
        should be used as a marker to indicate if the step should fail silently at validation.
    parent : BaseStep | None
        The parent step in the pipeline.
    """

    def __init__(
        self,
        fail_silently: bool = False,
        parent: StepProto | None = None,
    ) -> None:
        self._parent = parent
        self._fail_silently = fail_silently

    def __rrshift__(self, other: StepProto) -> StepProto:
        """
        Syntactic sugar for chaining steps together.

        Parameters
        ----------
        other : BaseStep
            The step to chain with.

        Returns
        -------
        BaseStep
            Rettuns self with with other as the parent.
        """
        self._parent = other
        return self

    def __call__(self) -> Iterator[SubjectData]:
        """
        Starts the pipeline. and processes on subject at a time.

        Yields
        ------
        Iterator[SubjectData]
            An iterator of the subjects.
        """
        if self._parent is None:
            raise StopIteration

        for subject_data in self._parent():
            subject_data = self.pre_process(subject_data)

            self.validate(subject_data)
            subject_data = self.process(subject_data)
            if self.filter(subject_data):
                continue

            subject_data = self.post_process(subject_data)

            yield subject_data

    def pre_process(self, subject_data: SubjectData) -> SubjectData:
        """
        Pre-processes the subject data before validation and processing.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to process.

        Returns
        -------
        SubjectData
            The pre-processed subject data
        """
        return subject_data

    def validate(self, subject_data: SubjectData) -> None:
        """
        Validates the subject data before processing.

        Raises
        ------
        ValidationError
            If the validation fails.
        """
        pass

    def process(self, subject_data: SubjectData) -> SubjectData:
        """
        Processes the subject data.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to process.

        Returns
        -------
        SubjectData
            The processed subject data.
        """
        return subject_data

    def filter(self, subject_data: SubjectData) -> bool:
        """
        Filter out a subject with the corresponding data.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to filter.

        Returns
        -------
        bool
            True if the subject should be filtered out, False otherwise.
        """
        return False

    def post_process(self, subject_data: SubjectData) -> SubjectData:
        """
        Post-processes the subject data after processing.

        Parameters
        ----------
        subject_data : SubjectData
            The subject data to process.

        Returns
        -------
        SubjectData
            The post-processed subject data.
        """
        return subject_data
