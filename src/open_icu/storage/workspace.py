from pathlib import Path

from open_icu.storage.base import FilStorage


class WorkspaceDir(FilStorage):

    @property
    def contents(self) -> list[Path]:
        return list(self._path.rglob("*.parquet"))
