from typing import Any, Callable


def test_func(*args: Any, **kwargs: Any) -> Callable[..., Any]:
    def _test_func(*args: Any, **kwargs: Any) -> Any:
        return

    return _test_func


class TestCls:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return "test"
