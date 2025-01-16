from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import Iterator, TypeVar

import yaml
from pydantic import BaseModel

from open_icu.types.base import SubjectData
from open_icu.types.conf.concept import Concept

T = TypeVar("T", bound=BaseModel)


class BaseStep(ABC):
    def __init__(
        self, config_path: Path | None = None, concept_path: Path | None = None, parent: BaseStep | None = None
    ) -> None:
        self._parent = parent
        self._config_path = config_path
        self._concept_path = concept_path or config_path

        self._concepts = []
        if self._concept_path is not None:
            self._concepts = self._read_config(self._concept_path / "concepts", Concept)

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
