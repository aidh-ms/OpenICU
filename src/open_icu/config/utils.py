from pathlib import Path
from typing import TypeVar

import yaml

from open_icu.config.source import BaseModel

ConfigType = TypeVar("ConfigType", bound=BaseModel)


def load_yaml_configs(config_path: Path, config_type: type[ConfigType]) -> list[ConfigType]:
    configs: list[ConfigType] = []
    for config_file in config_path.rglob("*.*"):
        if not config_file.is_file():
            continue

        if config_file.suffix.lower() not in {".yml", ".yaml"}:
            continue

        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        configs.append(config_type(**config))

    return configs
