from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import Generic, Iterator, TypeVar

import yaml
from pydantic import BaseModel

from open_icu.types.base import SubjectData
from open_icu.types.conf.concept import ConceptConfig

T = TypeVar("T", bound=BaseModel)
C = TypeVar("C", bound=BaseModel)


class BaseStep(ABC, Generic[C]):
    def __init__(
        self,
        configs: Path | list[C] | None = None,
        concept_configs: Path | list[ConceptConfig] | None = None,
        parent: BaseStep | None = None,
    ) -> None:
        self._parent = parent

        self._config_path = configs if isinstance(configs, Path) else None
        self._concept_path = concept_configs if isinstance(concept_configs, Path) else None
        if self._concept_path is not None:
            self._concept_path = self._config_path

        self._concept_configs: list[ConceptConfig] = []
        if isinstance(concept_configs, list):
            self._concept_configs = concept_configs
        elif self._concept_path is not None:
            self._concept_configs = self._read_config(self._concept_path / "concepts", ConceptConfig)

    def __rrshift__(self, other: BaseStep) -> BaseStep:
        self._parent = other
        return self

    def __call__(self) -> Iterator[SubjectData]:
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
        if self._config_path is None:
            return []

        confs = []
        for conf in config_path.glob("*.yml"):
            with open(conf, "r") as f:
                confs.append(config_type(**yaml.safe_load(f)))

        return confs

    def pre_process(self, subject_data: SubjectData) -> SubjectData:
        return subject_data

    def validate(self, subject_data: SubjectData) -> None:
        pass

    def process(self, subject_data: SubjectData) -> SubjectData:
        return subject_data

    def filter(self, subject_data: SubjectData) -> bool:
        return False

    def post_process(self, subject_data: SubjectData) -> SubjectData:
        return subject_data
