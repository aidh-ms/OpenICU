"""Type introspection utilities for generic type resolution.

This module provides functions for extracting generic type parameters from
classes at runtime, which is useful for implementing generic base classes
that need to know their concrete type arguments.
"""

from types import get_original_bases
from typing import Any, get_args


def get_generic_type(cls: type, position: int = 0) -> type[Any]:
    """Extract generic type argument from a class at the given position.

    Args:
        cls: The class to extract the generic type from
        position: The position of the type argument (default: 0)

    Returns:
        The type at the specified position

    Raises:
        TypeError: If no generic type could be resolved
    """
    for base in get_original_bases(cls):
        if args := get_args(base):
            if position < len(args):
                return args[position]

    for parent in cls.__mro__[1:]:
        try:
            for base in get_original_bases(parent):
                if args := get_args(base):
                    if position < len(args):
                        return args[position]
        except AttributeError:
            continue

    raise TypeError(
        f"Could not resolve generic type at position {position} for '{cls.__name__}'. "
        "Ensure the class inherits from a parameterized generic base."
    )
