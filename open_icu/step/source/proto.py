from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, Protocol

if TYPE_CHECKING:
    from open_icu.step.source.conf import SourceConfig
    from open_icu.type.subject import SubjectData


class SamplerServiceProto(Protocol):
    """
    A protocol for the sampler service.
    """

    def __init__(self, source_config: SourceConfig, *args: Any, **kwargs: Any) -> None:
        ...

    def __call__(self, *args: Any, **kwargs: Any) -> Iterator[SubjectData]:
        ...
