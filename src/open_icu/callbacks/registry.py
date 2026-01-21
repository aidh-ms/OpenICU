from functools import wraps
from typing import Any, Callable, Hashable

from open_icu.callbacks.proto import CallbackProtocol
from open_icu.utils.name import camel_to_snake


class CallbackRegistry:
    """registry for callbacks."""
    def __init__(self) -> None:
        """Initialize the registry storage."""
        self._registry: dict[str, CallbackProtocol] = {}

    def __len__(self) -> int:
        """Return the number of registered items."""
        return len(self._registry)

    def __contains__(self, key: Hashable) -> bool:
        """Check if key exists using 'in' operator."""
        return key in self._registry

    def __repr__(self) -> str:
        """Return string representation of the registry."""
        return f"{self.__class__.__name__}(entries={len(self._registry)})"

    def register(self, key: str, value: CallbackProtocol, overwrite: bool = False) -> None:
        """Register a callbacks object.

        Args:
            key: Unique identifier for the callbacks object
            value: Callbacks object to register
            overwrite: If True, replace existing callbacks with same key
        """
        if overwrite or key not in self._registry:
            self._registry[key] = value

    def unregister(self, key: str) -> bool:
        """Remove a callbacks by key.

        Args:
            key: The callbacks key to remove

        Returns:
            True if the callbacks was removed, False if not found
        """
        if key in self._registry:
            del self._registry[key]
            return True
        return False

    def get(self, key: str, default: CallbackProtocol | None = None) -> CallbackProtocol | None:
        """Retrieve a callbacks by its key.

        Args:
            key: The callbacks key to retrieve
            default: Default value if callbacks not found
        Returns:
            The callbacks object or default if not found
        """
        return self._registry.get(key, default)

    def keys(self) -> list[str]:
        """Get all registered callbacks identifiers.

        Returns:
            List of callbacks identifier strings
        """
        return list(self._registry.keys())

    def values(self) -> list[CallbackProtocol]:
        """Get all registered callbacks objects.

        Returns:
            List of callbacks instances
        """
        return list(self._registry.values())

    def items(self) -> list[tuple[str, CallbackProtocol]]:
        """Get all key-callbacks pairs.

        Returns:
            List of (identifier, callbacks) tuples
        """
        return list(self._registry.items())

    def clear(self) -> None:
        """Remove all entries from the registry."""
        self._registry.clear()


registry = CallbackRegistry()


def register_callback_cls[T: type[CallbackProtocol]](cls: T) -> Callable[..., T]:
    name = camel_to_snake(cls.__name__)
    registry.register(name, cls)

    @wraps(cls)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        return cls(*args, **kwargs)

    return wrapper
