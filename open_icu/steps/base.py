from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, Generic, Iterator, TypeVar

import yaml
from pydantic import BaseModel

from open_icu.types.base import SubjectData
from open_icu.types.conf.concept import ConceptConfig

if TYPE_CHECKING:
    from open_icu.steps.exceptions import ValidationError  # noqa

T = TypeVar("T", bound=BaseModel)
C = TypeVar("C", bound=BaseModel)


class BaseStep(ABC, Generic[C]):
    """
    A base class for all steps in the pipeline.
    It follows the pattern of a pipe and filter architecture.

    The class is generic and expects a configuration class to be passed in.

    Parameters
    ----------
    configs : Path | list[C] | None
        A path to the configuration files or a list of configuration objects.
    concept_configs : Path | list[ConceptConfig] | None
        A path to the concept configuration files or a list of concept configuration objects.
    parent : BaseStep | None
        The parent step in the pipeline.
    """

    def __init__(
        self,
        configs: Path | list[C] | None = None,
        concept_configs: Path | list[ConceptConfig] | None = None,
        parent: BaseStep | None = None,
    ) -> None:
        self._parent = parent

        self._config_path = configs if isinstance(configs, Path) else None
        self._concept_path = concept_configs if isinstance(concept_configs, Path) else None
        if self._concept_path is None:
            self._concept_path = self._config_path

        self._concept_configs: list[ConceptConfig] = []
        if isinstance(concept_configs, list):
            self._concept_configs = concept_configs
        elif self._concept_path is not None:
            self._concept_configs = self._read_config(self._concept_path / "concepts", ConceptConfig)

    def __rrshift__(self, other: BaseStep) -> BaseStep:
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

    def _read_config(self, config_path: Path, config_type: type[T]) -> list[T]:
        """
        Utility function to read configuration files.

        Parameters
        ----------
        config_path : Path
            The path to the configuration files.
        config_type : type[T]
            The type of the configuration object for parsing.

        Returns
        -------
        list[T]
            A list of configuration objects.
        """
        if self._config_path is None:
            return []

        confs = []
        for conf in config_path.glob("*.yml"):
            with open(conf, "r") as f:
                confs.append(config_type(**yaml.safe_load(f)))

        return confs

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
