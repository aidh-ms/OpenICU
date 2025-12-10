from abc import ABCMeta
from typing import Hashable

from pydantic import computed_field
from open_icu.config.dataset.source.config.base import OpenICUBaseModel


class BaseConfig(OpenICUBaseModel, metaclass=ABCMeta):
    """Abstract base class for configuration models."""
    __key_fields__: tuple[str, ...] = ()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def key(self) -> Hashable:
        """Generate a unique key for the configuration instance."""
        if not self.__key_fields__:
            raise NotImplementedError(
                f"__key_fields__ must be defined in {self.__class__.__name__} to use 'key' property."
            )

        if len(self.__key_fields__) == 1:
            key = getattr(self, self.__key_fields__[0])
            assert isinstance(key, Hashable)
            return key

        return tuple(
            key
            for field in self.__key_fields__
            if isinstance((key := getattr(self, field)), Hashable)
        )