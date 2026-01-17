"""Registry system for managing configuration objects.

This module provides a generic registry class for storing, retrieving,
and persisting configuration objects, with support for loading from
and saving to YAML files.
"""

from abc import ABC
from pathlib import Path
from typing import cast

from pydantic import ValidationError

from open_icu.config.base import BaseConfig
from open_icu.logging import get_logger
from open_icu.utils.type import get_generic_type

logger = get_logger(__name__)


class BaseConfigRegistry[T: BaseConfig](ABC):
    """Generic registry for configuration objects.

    Stores configuration instances indexed by their unique identifiers.
    Provides methods for registration, retrieval, and batch loading/saving
    from/to YAML files.

    Type Parameters:
        T: The type of configuration objects to store (must inherit from BaseConfig)
    """
    def __init__(self) -> None:
        """Initialize the registry storage."""
        self._registry: dict[str, T] = {}

    def __len__(self) -> int:
        """Return the number of registered items."""
        return len(self._registry)

    def __contains__(self, identifiers: tuple[str, ...]) -> bool:
        """Check if key exists using 'in' operator."""
        identifier = self._config_type.build_identifier(identifiers)
        return identifier in self._registry

    def __repr__(self) -> str:
        """Return string representation of the registry."""
        return f"{self.__class__.__name__}(entries={len(self._registry)})"

    @property
    def _config_type(self) -> type[T]:
        """Get the configuration type from the generic type parameter.

        Returns:
            The configuration class type this registry manages
        """
        t = get_generic_type(self.__class__)
        return cast(type[T], t)

    def register(self, value: T, overwrite: bool = False) -> None:
        """Register a configuration object.

        Args:
            value: Configuration object to register
            overwrite: If True, replace existing configuration with same identifier
        """
        if overwrite or value.identifier not in self._registry:
            self._registry[value.identifier] = value

    def unregister(self, identifiers: tuple[str, ...]) -> bool:
        """Remove a configuration by identifier.

        Args:
            identifiers: The configuration identifier to remove

        Returns:
            True if the configuration was removed, False if not found
        """
        identifier = self._config_type.build_identifier(identifiers)
        if identifier in self._registry:
            del self._registry[identifier]
            return True
        return False

    def get(self, identifiers: tuple[str, ...], default: T | None = None) -> T | None:
        """Retrieve a configuration by its identifier components.

        Args:
            identifiers: Tuple of identifier components (e.g., (class_name, version, name))
            default: Default value if configuration not found

        Returns:
            The configuration object or default if not found
        """
        identifier = self._config_type.build_identifier(identifiers)
        return self._registry.get(identifier, default)

    def keys(self) -> list[str]:
        """Get all registered configuration identifiers.

        Returns:
            List of configuration identifier strings
        """
        return list(self._registry.keys())

    def values(self) -> list[T]:
        """Get all registered configuration objects.

        Returns:
            List of configuration instances
        """
        return list(self._registry.values())

    def items(self) -> list[tuple[str, T]]:
        """Get all identifier-configuration pairs.

        Returns:
            List of (identifier, configuration) tuples
        """
        return list(self._registry.items())

    def clear(self) -> None:
        """Remove all entries from the registry."""
        self._registry.clear()

    def load(
        self,
        file_path: Path,
        overwrite: bool = False,
        includes: list[str] | None = None,
        excludes: list[str] | None = None,
    ) -> None:
        """Load configurations from YAML files in a directory.

        Recursively searches for YAML files in the specified directory and
        loads them as configuration objects. Optionally filters configurations
        by name.

        Args:
            file_path: Directory path to search for configuration files
            overwrite: If True, replace existing configurations with same identifier
            includes: If specified, only load configurations with these identifiers
            excludes: If specified, skip configurations with these identifiers
        """
        for config in load_config(file_path, self._config_type):
            if (
                (excludes is not None and config.identifier in excludes) or
                (includes is not None and config.identifier not in includes)
            ):
                logger.info("Skip loading configuration: %s", config.identifier)
                continue

            self.register(config, overwrite=overwrite)
            logger.info("Loaded configuration: %s", config.identifier)


    def save(self, path: Path) -> None:
        """Save all registered configurations to YAML files.

        Creates a directory hierarchy under path and saves each configuration
        to a separate YAML file based on its identifier components.

        Args:
            path: Base directory path for saving configurations
        """
        path.mkdir(parents=True, exist_ok=True)
        for config in self._registry.values():
            config.save(path)


def load_config[T: BaseConfig](path: Path, config_type: type[T]) -> list[T]:
    """Load all configuration files of a specific type from a directory.

    Recursively searches for YAML files in the directory and attempts to
    load them as the specified configuration type. Silently skips files
    that fail to load.

    Args:
        path: Directory path to search for configuration files
        config_type: The configuration class to instantiate

    Returns:
        List of successfully loaded configuration objects
    """
    configs = []
    for file_path in path.rglob("*.*"):
        if (
            not file_path.is_file()
            or file_path.suffix.lower() not in {".yml", ".yaml"}
        ):
            continue

        try:
            config = config_type.load(file_path)
        except ValidationError:
            logger.warning("failed to load config from %s", file_path)
            continue

        configs.append(config)
    return configs
