"""Workspace directory management for intermediate data storage.

This module provides the WorkspaceDir class for managing workspace
directories used to store intermediate processing results.
"""

from pathlib import Path

from open_icu.storage.base import FileStorage


class WorkspaceDir(FileStorage):
    """Workspace directory for storing intermediate processing data.

    Extends FilStorage to provide specific functionality for workspace
    directories that typically contain Parquet files from data processing steps.
    """

    @property
    def content(self) -> list[Path]:
        """Get all Parquet files in the workspace.

        Returns:
            List of Path objects for all .parquet files in the workspace
        """
        return list(self._path.rglob("*.parquet"))  # TODO: make more informative
