from pathlib import Path

from open_icu.storage.base import FilStorage


class OpenICUProject(FilStorage):
    def __init__(
            self,
            dataset_path: Path,
            overwrite: bool = False,
    ) -> None:
        super().__init__(dataset_path, overwrite)
        # Create the project directory if it doesn't exist
        if not self._path.exists():
            self.datasets_path.mkdir(parents=True, exist_ok=True)
            self.workspace_path.mkdir(parents=True, exist_ok=True)

    @property
    def datasets_path(self) -> Path:
        return self._path / "datasets"

    @property
    def workspace_path(self) -> Path:
        return self._path / "workspace"

# add methods for creating datasets and workspace dir
