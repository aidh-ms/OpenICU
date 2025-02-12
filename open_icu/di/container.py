from pathlib import Path

from dependency_injector import containers

dynamic_container = containers.DynamicContainer()

_root = Path(__file__).parent.parent

modules = [
    ".".join(module_path.relative_to(_root.parent).with_suffix("").parts)
    for module_path in Path(__file__).parent.parent.rglob("*.py")
    if module_path.name != "__init__.py"
]


def wire() -> None:
    dynamic_container.wire(modules=modules)
