"""MEDS dataset storage and metadata management.

This module provides the MEDSDataset class for managing Medical Event Data
Standard (MEDS) format datasets, including directory structure, metadata
generation, and code vocabulary extraction.
"""

import json
from datetime import datetime
from pathlib import Path

import polars as pl
from meds._version import __version__ as meds_version
from meds.schema import DatasetMetadataSchema

from open_icu.storage.base import FilStorage


class MEDSDataset(FilStorage):
    """MEDS format dataset storage manager.

    Manages a MEDS-compliant dataset directory structure with separate
    data and metadata subdirectories. Provides methods for writing
    dataset metadata and extracting code vocabularies.

    Attributes:
        data_path: Path to the data subdirectory
        metadata_path: Path to the metadata subdirectory
    """

    def __init__(
            self,
            dataset_path: Path,
            overwrite: bool = False,
    ) -> None:
        """Initialize the MEDS dataset storage.

        Args:
            dataset_path: Base path for the MEDS dataset
            overwrite: If True, remove existing dataset before creating
        """
        super().__init__(dataset_path, overwrite)
        # Create the MEDS dataset directory if it doesn't exist
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.metadata_path.mkdir(parents=True, exist_ok=True)

    @property
    def data_path(self) -> Path:
        """Get the data subdirectory path.

        Returns:
            Path to the data directory where MEDS Parquet files are stored
        """
        return self._path / "data"

    @property
    def metadata_path(self) -> Path:
        """Get the metadata subdirectory path.

        Returns:
            Path to the metadata directory containing dataset.json and codes.parquet
        """
        return self._path / "metadata"

    def write_metadata(self, metadata: dict) -> None:
        """Write dataset metadata to dataset.json.

        Automatically adds ETL information (OpenICU version, MEDS version,
        creation timestamp) to the provided metadata and validates against
        the MEDS schema before writing.

        Args:
            metadata: Dictionary of dataset metadata (e.g., dataset_name, dataset_version)

        Raises:
            ValidationError: If metadata doesn't conform to MEDS DatasetMetadataSchema
        """
        _metadata = {
            "etl_name": "OpenICU",
            "etl_version": "1.0.0",
            "meds_version": meds_version,
            "created_at": datetime.now().isoformat(),
        }

        metadata.update(_metadata)
        DatasetMetadataSchema.validate(metadata)

        with open(self.metadata_path / "dataset.json", "w") as f:
            json.dump(metadata, f, indent=4)

    def write_codes(self) -> None:
        """Extract and write the code vocabulary to codes.parquet.

        Scans all Parquet files in the data directory, extracts unique codes,
        and writes them to metadata/codes.parquet with description and
        parent_codes columns (set to null). This creates the MEDS-required
        code vocabulary file.
        """
        dfs = []
        for file_path in self.data_path.rglob("*.parquet"):
            _df = pl.scan_parquet(file_path).select(pl.col("code")).unique().collect(engine="streaming")
            dfs.append(_df)

        codes_df = pl.DataFrame(
            {
                "code": pl.Series([], dtype=pl.Utf8),
                "description": pl.Series([], dtype=pl.Utf8),
                "code_type": pl.Series([], dtype=pl.Utf8),
            }
        )
        if dfs:
            codes_df = pl.concat(dfs).unique().with_columns([
                pl.lit(None).alias("description").cast(pl.String),
                pl.lit(None).alias("parent_codes").cast(pl.String)
            ])

        codes_df.write_parquet(self.metadata_path / "codes.parquet")
