from __future__ import annotations

import re
from functools import wraps
from typing import Any, Callable

from open_icu.transform.callbacks.proto import CallbackProtocol

CAMEL_CASE_PATTERN = re.compile(r'(?<!^)(?=[A-Z])')

def register_callback_class(cls: type[CallbackProtocol]) -> Callable[..., CallbackProtocol]:
    name = CAMEL_CASE_PATTERN.sub('_', cls.__name__).lower()
    CallbackRegistry().register(name, cls)

    @wraps(cls)
    def wrapper(*args: Any, **kwargs: Any) -> CallbackProtocol:
        return cls(*args, **kwargs)
    return wrapper


class CallbackRegistry:
    _instance: CallbackRegistry | None = None
    _callbacks: dict[str, type[CallbackProtocol]]

    def __new__(cls) -> CallbackRegistry:
        """Create or return the singleton instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._callbacks = {}
        return cls._instance

    def register(
        self,
        name: str,
        callback: type[CallbackProtocol],
        overwrite: bool = False,
    ) -> None:
        if not overwrite and name in self._callbacks:
            return

        self._callbacks[name] = callback

    def unregister(self, name: str) -> None:
        del self._callbacks[name]

    def get(self, name: str) -> type[CallbackProtocol] | None:
        return self._callbacks.get(name)

    def all(self) -> dict[str, type[CallbackProtocol]]:
        return self._callbacks.copy()

    def clear(self) -> None:
        self._callbacks.clear()

    def __contains__(self, name: str) -> bool:
        return name in self._callbacks

    def __len__(self) -> int:
        return len(self._callbacks)
