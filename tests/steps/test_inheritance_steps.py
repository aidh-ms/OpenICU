"""End-to-end tests for dataset config inheritance across both pipeline steps.

Builds a "testdb-demo" dataset that extends the synthetic "testdb" fixtures:
the demo provides no table or concept configs of its own except a file-path
override for vitals and a tombstone for measurements — mirroring how a real
demo dataset (e.g. eICU demo) differs from its full counterpart.
"""

import shutil
from pathlib import Path

import polars as pl
import pytest

from open_icu import ConceptStep, ExtractionStep, OpenICUProject


@pytest.fixture
def demo_dirs(tmp_path: Path, data_dir: Path, table_config_dir: Path, concept_config: Path) -> None:
    """Create demo data + extends-based demo configs next to the testdb fixtures."""
    # Demo data: same tables, but the vitals file is named differently.
    demo_data = tmp_path / "data" / "testdb-demo"
    demo_data.mkdir(parents=True)
    shutil.copy(data_dir / "vitals.csv", demo_data / "vitals_demo.csv")
    shutil.copy(data_dir / "items.csv", demo_data / "items.csv")
    shutil.copy(data_dir / "measurements.csv", demo_data / "measurements.csv")

    # Demo config: extends testdb 1.0; only the diff is spelled out.
    demo_version_dir = tmp_path / "config" / "testdb-demo" / "1.0"
    (demo_version_dir / "tables").mkdir(parents=True)
    (demo_version_dir / "extends.yml").write_text("dataset: testdb\nversion: '1.0'\n")
    (demo_version_dir / "tables" / "vitals.yml").write_text("path: vitals_demo.csv\n")
    (demo_version_dir / "tables" / "measurements.yml").write_text("deleted: true\n")

    # Step configs covering both datasets.
    (tmp_path / "extraction.yml").write_text(
        f"""\
name: Extraction
version: 1.0.0

config_files:
  - path: {table_config_dir}
  - path: {demo_version_dir / "tables"}

config:
  data:
    - name: testdb
      path: {data_dir}
    - name: testdb-demo
      path: {demo_data}
"""
    )
    (tmp_path / "concept.yml").write_text(
        f"""\
name: Concept
version: 1.0.0

config_files:
  - path: {tmp_path / "config" / "concepts"}

config:
  extraction_step: Extraction
  dataset_configs:
    - name: testdb
      path: {tmp_path / "config" / "testdb" / "1.0" / "mappings"}
    - name: testdb-demo
      path: {demo_version_dir / "mappings"}
"""
    )


@pytest.fixture
def project(tmp_path: Path, demo_dirs: None) -> OpenICUProject:
    project = OpenICUProject(tmp_path / "project")
    ExtractionStep.load(project, tmp_path / "extraction.yml").run()
    ConceptStep.load(project, tmp_path / "concept.yml").run()
    return project


class TestInheritedExtraction:
    def test_inherited_table_with_path_override(self, project: OpenICUProject) -> None:
        output = project.datasets_path / "extraction" / "data" / "testdb-demo" / "1.0" / "vitals" / "CHART.parquet"
        assert output.exists()

        df = pl.read_parquet(output)
        assert df.height == 4
        # codes carry the demo dataset's identity, not the base's
        assert "testdb-demo//vitals//220045//Heart Rate//bpm" in df["code"].to_list()

    def test_tombstoned_table_is_not_extracted(self, project: OpenICUProject) -> None:
        demo_data = project.datasets_path / "extraction" / "data" / "testdb-demo"
        assert not (demo_data / "1.0" / "measurements").exists()

    def test_base_dataset_is_unaffected(self, project: OpenICUProject) -> None:
        base_data = project.datasets_path / "extraction" / "data" / "testdb" / "1.0"
        assert (base_data / "vitals" / "CHART.parquet").exists()
        assert (base_data / "measurements" / "WEIGHT.parquet").exists()


class TestInheritedConcepts:
    def test_concept_mapping_is_inherited_by_demo(self, project: OpenICUProject) -> None:
        output = project.datasets_path / "concept" / "data" / "heart_rate" / "1.0.0" / "testdb-demo.parquet"
        assert output.exists()

        df = pl.read_parquet(output)
        assert df.height == 2
        assert df["code"].unique().to_list() == ["heart_rate//bpm"]
        assert df["dataset"].unique().to_list() == ["testdb-demo"]

    def test_base_concept_still_extracted(self, project: OpenICUProject) -> None:
        output = project.datasets_path / "concept" / "data" / "heart_rate" / "1.0.0" / "testdb.parquet"
        assert output.exists()
