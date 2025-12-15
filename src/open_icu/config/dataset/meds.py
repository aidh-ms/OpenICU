from __future__ import annotations

import json
from datetime import datetime
import logging
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory

from meds._version import __version__ as meds_version
from meds.schema import DatasetMetadataSchema
from open_icu.metrics.metrics import get_statistics, PipelineArtifacts as pa

# initilize statistics
statistics = get_statistics()
statistics.basicConfig("statistics.json")

# initialize logging
logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] [%(module)s] %(message)s",
    )
logging.info("Logging initialized.")


class MEDSDataset:
    def __init__(
            self,
            project_path: Path | str | None = None,
            overwrite: bool = False,
    ) -> None:
        temp_dir = None
        if project_path is None:
            temp_dir = TemporaryDirectory()
            project_path = temp_dir.name
            logging.info("Create temporary directory")

        self._project_path = Path(project_path)
        self._temp_dir = temp_dir

        # Remove existing project directory if overwrite is True
        if overwrite and self._project_path.exists():
            self.cleanup()

        # Create the project directory if it doesn't exist
        if not self.project_path.exists():
            self.data_path.mkdir(parents=True, exist_ok=True)
            self.metadata_path.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Initialized: {project_path}")

    def cleanup(self) -> None:
        if self._temp_dir:
            self._temp_dir.cleanup()
        elif self._project_path.exists():
            rmtree(self._project_path)
        logging.info("Clean up")

    @property
    def project_path(self) -> Path:
        return self._project_path

    @property
    def data_path(self) -> Path:
        return self._project_path / "data"

    @property
    def metadata_path(self) -> Path:
        return self._project_path / "metadata"

    def write_metadata(self, metadata: dict) -> None:
        _metadata = {
            "etl_name": "OpenICU",
            "etl_version": "0.0.1",
            "meds_version": meds_version,
            "created_at": datetime.now().isoformat(),
        }
        metadata.update(_metadata)
        DatasetMetadataSchema.validate(metadata)

        with open(self.metadata_path / "dataset.json", "w") as f:
            json.dump(metadata, f, indent=4)

        logging.info(f"Write metadata: {_metadata}")
