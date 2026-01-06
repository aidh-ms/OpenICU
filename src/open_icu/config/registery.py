from abc import ABC
from pathlib import Path
from typing import cast

from open_icu.config.base import BaseConfig
from open_icu.utils.type import get_generic_type


class BaseConfigRegistery[T: BaseConfig](ABC):
    def __init__(self) -> None:
        """Initialize the registry storage."""
        self._registry: dict[str, T] = {}

    @property
    def _config_type(self) -> type[T]:
        t = get_generic_type(self.__class__)
        return cast(type[T], t)

    def register(self, value: T, overwrite: bool = False) -> None:
        if overwrite or value.identifier not in self._registry:
            self._registry[value.identifier] = value

    def unregister(self, key: str) -> bool:
        if key in self._registry:
            del self._registry[key]
            return True
        return False

    def get(self, identifiers: tuple[str, ...], default: T | None = None) -> T | None:
        identifier = self._config_type.build_identifier(identifiers)
        return self._registry.get(identifier, default)

    def keys(self) -> list[str]:
        return list(self._registry.keys())

    def values(self) -> list[T]:
        return list(self._registry.values())

    def items(self) -> list[tuple[str, T]]:
        return list(self._registry.items())

    def load(
        self,
        file_path: Path,
        overwrite: bool = False,
        includes: list[str] | None = None,
        excludes: list[str] | None = None,
    ) -> None:
        for config in load_config(file_path, self._config_type):
            if excludes is not None and config.name in excludes:
                continue
            if includes is not None and config.name not in includes:
                continue

            self.register(config, overwrite=overwrite)


    def save(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        for config in self._registry.values():
            config.save(path)


def load_config[T: BaseConfig](path: Path, config_type: type[T]) -> list[T]:
    configs = []
    for file_path in path.rglob("*.*"):
        if (
            not file_path.is_file()
            or file_path.suffix.lower() not in {".yml", ".yaml"}
        ):
            continue

        try:
            config = config_type.load(file_path)
        except Exception:
            continue  # TODO: log error

        configs.append(config)
    return configs
