"""Abstract base registry class with singleton pattern."""
from __future__ import annotations

from abc import ABC
import logging
from pathlib import Path
from typing import Hashable

import yaml

from open_icu.helper.config import BaseConfig
from open_icu.helper.singelton import SingletonABCMeta

logger = logging.getLogger(__name__) 

class BaseRegistry[T](ABC, metaclass=SingletonABCMeta):
    """Abstract base class for registries with singleton pattern.

    Each subclass of BaseRegistry will be a singleton, meaning only one
    instance of each subclass can exist. The registry stores key-value pairs
    where keys can be any hashable value.

    Type Parameters:
        T: The type of values stored in the registry

    Examples:
        >>> class ConfigRegistry(BaseRegistry[dict]):
        ...     pass
        ...
        >>> registry1 = ConfigRegistry()
        >>> registry2 = ConfigRegistry()
        >>> registry1 is registry2  # Same instance
        True
        >>> registry1.register(("app", "settings"), {"timeout": 30})
        >>> registry1.get(("app", "settings"))
        {'timeout': 30}
    """

    def __init__(self) -> None:
        """Initialize the registry storage."""
        self._registry: dict[Hashable, T] = {}

    def register(self, key: Hashable, value: T) -> None:
        """Register a value with a key.

        Args:
            key: Registry key (hashable value)
            value: Value to register

        Raises:
            ValueError: If key already exists in registry
        """
        if key in self._registry:
            raise ValueError(f"Key {key} is already registered")

        self._registry[key] = value
    def get(self, key: Hashable, default: T | None = None) -> T | None:
        """Retrieve a value by key.

        Args:
            key: Registry key (hashable value)
            default: Default value if key not found

        Returns:
            The registered value or default if not found
        """
        return self._registry.get(key, default)

    def unregister(self, key: Hashable) -> bool:
        """Remove a key from the registry.

        Args:
            key: Registry key (hashable value)

        Returns:
            True if key was removed, False if key didn't exist
        """
        if key in self._registry:
            del self._registry[key]
            return True
        return False

    def clear(self) -> None:
        """Remove all entries from the registry."""
        self._registry.clear()

    def keys(self) -> list[Hashable]:
        """Return all registered keys.

        Returns:
            List of all registry keys
        """
        return list(self._registry.keys())

    def values(self) -> list[T]:
        """Return all registered values.

        Returns:
            List of all registered values
        """
        return list(self._registry.values())

    def items(self) -> list[tuple[Hashable, T]]:
        """Return all key-value pairs.

        Returns:
            List of (key, value) tuples
        """
        return list(self._registry.items())

    def __len__(self) -> int:
        """Return the number of registered items."""
        return len(self._registry)

    def __contains__(self, key: Hashable) -> bool:
        """Check if key exists using 'in' operator."""
        return key in self._registry

    def __getitem__(self, key: Hashable) -> T:
        """Enable bracket notation for getting items."""
        return self._registry[key]

    def __delitem__(self, key: Hashable) -> None:
        """Enable bracket notation for deleting items."""
        del self._registry[key]

    def __setitem__(self, key: Hashable, value: T) -> None:
        """Enable bracket notation for setting items."""
        self._registry[key] = value

    def __repr__(self) -> str:
        """Return string representation of the registry."""
        return f"{self.__class__.__name__}(entries={len(self._registry)})"


class BaseConfigRegistry[T: BaseConfig](BaseRegistry[T], metaclass=SingletonABCMeta):
    """Abstract base class for configuration registries."""

    config_class: type[T]
    config_file_name: str = "*"

    @classmethod
    def from_path(cls, path: str | Path | list[str | Path]) -> BaseConfigRegistry[T]:
        """Create registry from configuration files at given path(s).

        Args:
            path: Path or list of paths to configuration files

        Returns:
            An instance of the configuration registry
        """
        registry = cls()

        paths: list[Path]
        if isinstance(path, (str, Path)):
            paths = [Path(path)]
        elif isinstance(path, list):
            paths = [Path(p) for p in path]

        config_paths = []
        for path in paths:
             for config_path in path.rglob("dataset.*"):
                if (
                    not config_path.is_file()
                    or config_path.suffix.lower() not in {".yml", ".yaml"}
                ):
                    continue
                config_paths.append(config_path)

        for config in registry._parse_configs(config_paths):
            registry.register(config.key, config)


        logger.info(f"Registry: {registry}")

        return registry

    def _parse_configs(self, paths: list[Path]) -> list[T]:
        """Parse configuration file into list of config instances.

        Args:
            file_path: Path to configuration file

        Returns:
            List of configuration instances
        """
        configs: list[T] = []
        for file_path in paths:
            with open(file_path, "r") as f:
                config_data = yaml.safe_load(f)

            configs.append(self.config_class(**config_data))
        logger.info(f"Loaded {len(configs)} config files")
        return configs
