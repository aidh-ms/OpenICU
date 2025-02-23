from abc import ABCMeta
from pathlib import Path

from open_icu.step.base import BaseStep
from open_icu.step.proto import StepProto


class BaseSinkStep(BaseStep, metaclass=ABCMeta):
    """
    A base sink step.
    """

    def __init__(self, sink_path: Path, fail_silently: bool = False, parent: StepProto | None = None) -> None:
        super().__init__(fail_silently, parent)

        self._sink_path: Path = sink_path
