from abc import ABCMeta
from pathlib import Path

from open_icu.step.base import BaseStep
from open_icu.step.proto import StepProto


class BaseSinkStep(BaseStep, metaclass=ABCMeta):
    """
    A base sink step.

    Parameters
    ----------
    sink_path : Path
        The path to the sink directory.
    fail_silently : bool, default: False
        Whether to fail silently or not.
    parent : BaseStep | None
        The parent step.
    """

    def __init__(self, sink_path: Path, fail_silently: bool = False, parent: StepProto | None = None) -> None:
        super().__init__(fail_silently, parent)

        self._sink_path: Path = sink_path
