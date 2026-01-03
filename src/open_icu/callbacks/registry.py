import re
from abc import ABC, ABCMeta
from functools import wraps
from typing import Any, Callable, Hashable

from open_icu.callbacks.proto import CallbackProtocol


# TODO
class SingletonMeta(type):
    """Metaclass that implements singleton pattern for all child classes.

    Each class that uses this metaclass will have exactly one instance.
    Subclasses of a singleton class will have their own singleton instance.
    """

    _instances: dict[type, Any] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """Create or return the singleton instance for this class.

        Args:
            *args: Positional arguments for class initialization
            **kwargs: Keyword arguments for class initialization

        Returns:
            The singleton instance of this class
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class SingletonABCMeta(SingletonMeta, ABCMeta):
    """Metaclass combining singleton pattern with abstract base class.

    This allows creating abstract base classes where each concrete
    subclass is a singleton.
    """

    pass


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





CAMEL_CASE_PATTERN = re.compile(r'(?<!^)(?=[A-Z])')

def register_callback_class(cls: type[CallbackProtocol]) -> Callable[..., CallbackProtocol]:
    name = CAMEL_CASE_PATTERN.sub('_', cls.__name__).lower()
    CallbackRegistry().register(name, cls)

    @wraps(cls)
    def wrapper(*args: Any, **kwargs: Any) -> CallbackProtocol:
        return cls(*args, **kwargs)
    return wrapper


class CallbackRegistry(BaseRegistry[type[CallbackProtocol]]):
    pass
