from __future__ import annotations

import re
from functools import wraps
from typing import Any, Callable

from open_icu.helper.registry import BaseRegistry
from open_icu.transform.callbacks.proto import CallbackProtocol

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
