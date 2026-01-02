from abc import ABC

from open_icu.steps.base.step import BaseStep


class StepRegistery(ABC):
    def __init__(self) -> None:
        """Initialize the registry storage."""
        self._registry: dict[str, type[BaseStep]] = {}

    def register(self, key: str, value: type[BaseStep], overwrite: bool = False) -> None:
        if overwrite or value.name in self._registry:
            self._registry[key] = value

    def unregister(self, key: str) -> bool:
        if key in self._registry:
            del self._registry[key]
            return True
        return False

    def get(self, key: str, default: type[BaseStep] | None = None) -> type[BaseStep] | None:
        return self._registry.get(key, default)

    def keys(self) -> list[str]:
        return list(self._registry.keys())

    def values(self) -> list[type[BaseStep]]:
        return list(self._registry.values())

    def items(self) -> list[tuple[str, type[BaseStep]]]:
        return list(self._registry.items())


registery = StepRegistery()

def register_step[T: BaseStep](step: type[T], overwrite: bool = False) -> type[T]:
    registery.register(step.__class__.__name__, step, overwrite)
    return step
