from pathlib import Path
from shutil import rmtree


class FilStorage:
    def __init__(
            self,
            path: Path,
            overwrite: bool = False,
    ) -> None:
        self._path = path

        # Remove existing project directory if overwrite is True
        if overwrite and self._path.exists():
            self.cleanup()
        self._path.mkdir(parents=True, exist_ok=True)

    def cleanup(self) -> None:
        if self._path.exists():
            rmtree(self._path)

    @property
    def path(self) -> Path:
        return self._path
