from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from open_icu.type.subject import SubjectData

if TYPE_CHECKING:
    from open_icu.step.cohort.conf import CohortConfig


class ICohortService(Protocol):
    """
    A protocol for the sampler service.
    """

    def __init__(self, cohort_config: CohortConfig, *args: Any, **kwargs: Any) -> None:
        ...

    def __call__(self, subject_data: SubjectData, *args: Any, **kwargs: Any) -> bool:
        ...
