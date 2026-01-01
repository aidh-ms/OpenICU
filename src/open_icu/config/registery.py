from abc import ABC
from pathlib import Path
from types import get_original_bases

from typing_extensions import get_args

from open_icu.config.base import BaseConfig


class BaseConfigRegistery[T: BaseConfig](ABC):
    def __init__(self) -> None:
        """Initialize the registry storage."""
        self._registry: dict[str, T] = {}

    @property
    def _config_type(self) -> type[T]:
        base = get_original_bases(self.__class__)[0]
        if not (types := get_args(base)):
           raise TypeError(f"Could not resolve generic type for '{self.__class__.__name__}'.")

        return types[0]

    def register(self, value: T, overwrite: bool = False) -> None:
        if overwrite or value.identifier in self._registry:
            self._registry[value.identifier] = value

    def unregister(self, key: str) -> bool:
        if key in self._registry:
            del self._registry[key]
            return True
        return False

    def get(self, name: str, version: str, default: T | None = None) -> T | None:
        identifier = self._config_type.build_identifier(name, version)
        return self._registry.get(identifier, default)

    def keys(self) -> list[str]:
        return list(self._registry.keys())

    def values(self) -> list[T]:
        return list(self._registry.values())

    def items(self) -> list[tuple[str, T]]:
        return list(self._registry.items())

    def load(self, file_path: Path, overwrite: bool = False) -> None:
        for file_path in file_path.rglob("*.*"):
            if (
                not file_path.is_file()
                or file_path.suffix.lower() not in {".yml", ".yaml"}
            ):
                continue

            config = self._config_type.load(file_path)
            self.register(config, overwrite=overwrite)
