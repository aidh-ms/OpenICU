"""Base file storage class for managing directory-based storage.

This module provides the foundational file storage abstraction used
throughout OpenICU for managing directories with optional cleanup.
"""

from pathlib import Path
from shutil import rmtree

from open_icu.logging import get_logger

logger = get_logger(__name__)


class FileStorage:
    """Base class for file system storage management.

    Manages a directory path with support for initialization, cleanup,
    and optional overwrite behavior. All storage classes in OpenICU
    inherit from this base class.

    Attributes:
        path: The managed directory path
    """
    def __init__(
            self,
            path: Path,
            overwrite: bool = False,
    ) -> None:
        """Initialize the file storage.

        Args:
            path: Directory path to manage
            overwrite: If True, remove existing directory before creating
        """
        self._path = path

        logger.debug(
            "Initializing storage at %s with overwrite=%s",
            path,
            overwrite,
        )

        # Remove existing project directory if overwrite is True
        if overwrite:
            logger.info("Overwriting existing directory at %s", self._path)
            self.cleanup()

        self._path.mkdir(parents=True, exist_ok=True)

    def cleanup(self) -> None:
        """Remove the managed directory and all its contents.

        Uses shutil.rmtree to recursively delete the directory if it exists.
        """
        if self._path.exists():
            logger.info("Removing directory at %s", self._path)
            rmtree(self._path)

    @property
    def path(self) -> Path:
        """Get the managed directory path.

        Returns:
            The Path object for the managed directory
        """
        return self._path
