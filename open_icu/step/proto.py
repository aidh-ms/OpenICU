from __future__ import annotations

from typing import Iterator, Protocol

from open_icu.type.subject import SubjectData


class StepProto(Protocol):
    def __init__(self) -> None:
        ...

    def __rrshift__(self, other: StepProto) -> StepProto:
        ...

    def __call__(self) -> Iterator[SubjectData]:
        ...

    def pre_process(self, subject_data: SubjectData) -> SubjectData:
        ...

    def validate(self, subject_data: SubjectData) -> None:
        ...

    def process(self, subject_data: SubjectData) -> SubjectData:
        ...

    def filter(self, subject_data: SubjectData) -> bool:
        ...

    def post_process(self, subject_data: SubjectData) -> SubjectData:
        ...
