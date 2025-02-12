from importlib import import_module
from pathlib import Path
from typing import Any, Callable, TypeVar

import yaml

from open_icu.conf.base import Configuration


def import_callable(dotted_path: str) -> Callable[..., Callable[..., Any]]:
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
    ```pycon
    class CallableClass:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            ...

        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            ...
    ```

    The function should return a callable.
    ```pycon
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


ConfType = TypeVar("ConfType", bound=Configuration)


def load_yaml_configs(config_path: Path, config_type: type[ConfType]) -> list[ConfType]:
    """
    Utility function to load YAML configuration files.

    Parameters
    ----------
    config_path : Path
        The path to the configuration files.
    config_type : type[ConfType]
        The type of the configuration object.

    Returns
    -------
    list[ConfType]
        A list of configuration objects.
    """
    configs: list[ConfType] = []
    for config_file in config_path.rglob("*.*"):
        if not config_file.is_file():
            continue

        if config_file.suffix.lower() not in {".yaml", ".yml"}:
            continue

        with open(config_file, "r") as f:
            configs.append(config_type(**yaml.safe_load(f)))

    return configs
