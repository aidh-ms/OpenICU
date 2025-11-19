"""Singleton metaclass for registry pattern."""

from abc import ABCMeta
from typing import Any


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
