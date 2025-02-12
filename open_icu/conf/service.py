from typing import Any, Generic, TypeVar, cast

from pydantic import ConfigDict, Field, computed_field

from open_icu.conf.base import Configuration
from open_icu.conf.proto import ServiceProto
from open_icu.conf.utils import import_callable

T = TypeVar("T", bound=ServiceProto)


class ServiceConfiguration(Configuration, Generic[T]):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    path: str

    @computed_field  # type: ignore
    @property
    def service(self) -> T:
        service = import_callable(self.path)(self, *self.args, **self.kwargs)
        assert callable(service)
        return cast(T, service)
