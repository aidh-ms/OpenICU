"""Utilities for locating OpenICU's shipped configuration directory."""

from importlib.resources import files
from pathlib import Path


def get_config_root() -> Path | None:
    """Return the root directory containing OpenICU's shipped configs.

    Installed packages contain the configs inside ``open_icu/configs``.
    During development, they live in the repository-level ``configs``
    directory.
    """
    module_path = Path(str(files("open_icu")))

    package_config_path = module_path / "configs"
    if package_config_path.is_dir():
        return package_config_path

    dev_config_path = module_path.parent.parent / "configs"
    if dev_config_path.is_dir():
        return dev_config_path

    return None
