import json
from datetime import datetime
from pathlib import Path

import polars as pl
from meds._version import __version__ as meds_version
from meds.schema import DatasetMetadataSchema

from open_icu.storage.base import FilStorage


class MEDSDataset(FilStorage):
    def __init__(
            self,
            dataset_path: Path,
            overwrite: bool = False,
    ) -> None:
        super().__init__(dataset_path, overwrite)
        # Create the MEDS dataset directory if it doesn't exist
        if not self._path.exists():
            self.data_path.mkdir(parents=True, exist_ok=True)
            self.metadata_path.mkdir(parents=True, exist_ok=True)

    @property
    def data_path(self) -> Path:
        return self._path / "data"

    @property
    def metadata_path(self) -> Path:
        return self._path / "metadata"

    def write_metadata(self, metadata: dict) -> None:
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
        dfs = []
        for file_path in self.data_path.rglob("*.parquet"):
            _df = pl.scan_parquet(file_path).select(pl.col("code")).unique().collect("streaming")
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
