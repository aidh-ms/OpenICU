from pathlib import Path

from open_icu.storage.base import FilStorage
from open_icu.storage.meds import MEDSDataset
from open_icu.storage.workspace import WorkspaceDir


class OpenICUProject(FilStorage):
    def __init__(
            self,
            path: Path,
            overwrite: bool = False,
    ) -> None:
        super().__init__(path, overwrite)
        # Create the project directory if it doesn't exist
        if not self._path.exists():
            self.datasets_path.mkdir(parents=True, exist_ok=True)
            self.workspace_path.mkdir(parents=True, exist_ok=True)
            self.configs_path.mkdir(parents=True, exist_ok=True)

        self._datasets = {}
        self._workspace = {}

    def __enter__(self) -> "OpenICUProject":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        pass

    @property
    def datasets_path(self) -> Path:
        return self._path / "datasets"

    @property
    def workspace_path(self) -> Path:
        return self._path / "workspace"

    @property
    def configs_path(self) -> Path:
        return self._path / "configs"

    @property
    def workspace(self) -> dict[str, WorkspaceDir]:
        return self._workspace

    @property
    def datasets(self) -> dict[str, MEDSDataset]:
        return self._datasets

    def add_workspace_dir(self, name: str, overwrite: bool = False) -> WorkspaceDir:
        dir_path = self.workspace_path / name

        workspace_dir = WorkspaceDir(dir_path, overwrite=overwrite)
        self._workspace[name] = workspace_dir
        return workspace_dir

    def add_dataset(self, name: str, overwrite: bool = False) -> MEDSDataset:
        dataset_path = self.datasets_path / name

        dataset = MEDSDataset(dataset_path, overwrite=overwrite)
        self._datasets[name] = dataset
        return dataset
