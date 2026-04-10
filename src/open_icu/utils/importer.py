from importlib import import_module
from typing import Any, Callable


def import_callable(dotted_path: str) -> Any:
    """
    Import a callable that returns a callable from a dotted path.
    This can represent a class or a function that returns a callable.

    Parameters
    ----------
    dotted_path : str
        The dotted path to the callable.

    Returns
    -------
    callable
        The imported callable that returns a callable.

    Examples
    --------
    The callable can be a class or a function that returns a callable.
    The class should have an `__init__` method and a `__call__` method.
    ```py
    class CallableClass:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            ...

        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            ...
    ```

    The function should return a callable.
    ```py
    def callable_func(*args: Any, **kwargs: Any) -> Callable[..., Any]:
        def _callable_func(*args: Any, **kwargs: Any) -> Any:
            ...
        return _callable_func
    ```
    """
    module_name, cls_name = dotted_path.rsplit(".", 1)
    module = import_module(module_name)
    _callable: Callable[..., Callable[..., Any]] = getattr(module, cls_name)
    return _callable
