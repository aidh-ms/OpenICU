from __future__ import annotations

from pathlib import Path


class Pipeline:
    def __init__(self) -> None:
        pass

    @classmethod
    def from_config_path(cls, path: str | Path) -> Pipeline:
        return cls()

    def run(self) -> None:
        pass
