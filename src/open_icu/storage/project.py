"""OpenICU project management and directory structure.

This module provides the OpenICUProject class for managing the complete
project directory structure, including datasets, workspace directories,
and configuration files.
"""

from pathlib import Path

from open_icu.storage.base import FileStorage
from open_icu.storage.meds import MEDSDataset
from open_icu.storage.workspace import WorkspaceDir


class OpenICUProject(FileStorage):
    """OpenICU project directory manager.

    Manages the complete OpenICU project structure with separate directories
    for datasets (MEDS format), workspace (intermediate files), and configs
    (YAML configuration files). Supports context manager protocol for
    resource management.

    Directory structure:
        - datasets/: MEDS format output datasets
        - workspace/: Intermediate processing files
        - configs/: Configuration YAML files

    Attributes:
        datasets_path: Path to the datasets directory
        workspace_path: Path to the workspace directory
        configs_path: Path to the configs directory
        datasets: Dictionary of managed MEDS datasets
        workspace: Dictionary of managed workspace directories
    """

    def __init__(
            self,
            path: Path,
            overwrite: bool = False,
    ) -> None:
        """Initialize the OpenICU project.

        Args:
            path: Base path for the project
            overwrite: If True, remove existing project before creating
        """
        super().__init__(path, overwrite)
        # Create the project directory if it doesn't exist
        if not self._path.exists():
            self.datasets_path.mkdir(parents=True, exist_ok=True)
            self.workspace_path.mkdir(parents=True, exist_ok=True)
            self.configs_path.mkdir(parents=True, exist_ok=True)

        self._datasets = {}
        self._workspace = {}

    def __enter__(self) -> "OpenICUProject":
        """Enter context manager.

        Returns:
            Self for use in with statements
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Exit context manager.

        Args:
            exc_type: Exception type if an exception occurred
            exc_value: Exception value if an exception occurred
            traceback: Traceback if an exception occurred
        """
        pass

    @property
    def datasets_path(self) -> Path:
        """Get the datasets directory path.

        Returns:
            Path to the datasets subdirectory
        """
        return self._path / "datasets"

    @property
    def workspace_path(self) -> Path:
        """Get the workspace directory path.

        Returns:
            Path to the workspace subdirectory
        """
        return self._path / "workspace"

    @property
    def configs_path(self) -> Path:
        """Get the configs directory path.

        Returns:
            Path to the configs subdirectory
        """
        return self._path / "configs"

    @property
    def workspace(self) -> dict[str, WorkspaceDir]:
        """Get the dictionary of managed workspace directories.

        Returns:
            Dictionary mapping workspace names to WorkspaceDir objects
        """
        return self._workspace

    @property
    def datasets(self) -> dict[str, MEDSDataset]:
        """Get the dictionary of managed datasets.

        Returns:
            Dictionary mapping dataset names to MEDSDataset objects
        """
        return self._datasets

    def add_workspace_dir(self, name: str, overwrite: bool = False) -> WorkspaceDir:
        """Create and register a new workspace directory.

        Args:
            name: Name for the workspace directory
            overwrite: If True, remove existing workspace before creating

        Returns:
            The created WorkspaceDir instance
        """
        dir_path = self.workspace_path / name

        workspace_dir = WorkspaceDir(dir_path, overwrite=overwrite)
        self._workspace[name] = workspace_dir
        return workspace_dir

    def add_dataset(self, name: str, overwrite: bool = False) -> MEDSDataset:
        """Create and register a new MEDS dataset.

        Args:
            name: Name for the dataset
            overwrite: If True, remove existing dataset before creating

        Returns:
            The created MEDSDataset instance
        """
        dataset_path = self.datasets_path / name

        dataset = MEDSDataset(dataset_path, overwrite=overwrite)
        self._datasets[name] = dataset
        return dataset
