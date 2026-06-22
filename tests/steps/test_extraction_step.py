"""End-to-end tests for the extraction step on synthetic fixture data."""

from datetime import datetime
from pathlib import Path

import polars as pl

from open_icu import ExtractionStep, OpenICUProject


def run_extraction(tmp_path: Path, extraction_config: Path) -> OpenICUProject:
    project = OpenICUProject(tmp_path / "project")
    step = ExtractionStep.load(project, extraction_config)
    step.run()
    return project


class TestExtractionStep:
    def test_writes_meds_parquet_per_event(self, tmp_path: Path, extraction_config: Path) -> None:
        project = run_extraction(tmp_path, extraction_config)

        output = project.datasets_path / "extraction" / "data" / "testdb" / "1.0" / "vitals" / "CHART.parquet"
        assert output.exists()

        df = pl.read_parquet(output)
        assert df.height == 4
        assert df.schema["subject_id"] == pl.Int64
        assert df.schema["time"] == pl.Datetime(time_unit="us")
        assert df.schema["code"] == pl.String
        assert df.schema["numeric_value"] == pl.Float32
        assert df.schema["text_value"] == pl.String
        assert "stay_id" in df.columns  # extension column preserved

    def test_code_encodes_provenance(self, tmp_path: Path, extraction_config: Path) -> None:
        project = run_extraction(tmp_path, extraction_config)
        df = pl.read_parquet(
            project.datasets_path / "extraction" / "data" / "testdb" / "1.0" / "vitals" / "CHART.parquet"
        )

        codes = set(df["code"].to_list())
        assert "testdb//vitals//220045//Heart Rate//bpm" in codes
        assert "testdb//vitals//220050//Systolic BP//mmHg" in codes
        # unmatched join keys: the null label is skipped in the code, not rendered
        assert "testdb//vitals//999999//units" in codes

    def test_join_and_values(self, tmp_path: Path, extraction_config: Path) -> None:
        project = run_extraction(tmp_path, extraction_config)
        df = pl.read_parquet(
            project.datasets_path / "extraction" / "data" / "testdb" / "1.0" / "vitals" / "CHART.parquet"
        ).sort("time")

        heart_rates = df.filter(pl.col("code").str.contains("Heart Rate"))
        assert heart_rates["numeric_value"].to_list() == [80.0, 82.0]
        assert heart_rates["text_value"].to_list() == ["eighty", None]
        assert heart_rates["time"].to_list() == [
            datetime(2024, 1, 1, 8, 0),
            datetime(2024, 1, 1, 9, 0),
        ]

    def test_multiple_events_per_table(self, tmp_path: Path, extraction_config: Path) -> None:
        project = run_extraction(tmp_path, extraction_config)
        base = project.datasets_path / "extraction" / "data" / "testdb" / "1.0" / "measurements"

        weight = pl.read_parquet(base / "WEIGHT.parquet")
        height = pl.read_parquet(base / "HEIGHT.parquet")
        assert weight["code"].unique().to_list() == ["testdb//measurements//kg"]
        assert weight["numeric_value"].to_list() == [80.0, 60.0]
        assert height["numeric_value"].to_list() == [2.0, 1.5]

    def test_metadata_written(self, tmp_path: Path, extraction_config: Path) -> None:
        project = run_extraction(tmp_path, extraction_config)
        metadata_path = project.datasets_path / "extraction" / "metadata"

        assert (metadata_path / "dataset.json").exists()
        codes = pl.read_parquet(metadata_path / "codes.parquet")
        assert "testdb//vitals//220045//Heart Rate//bpm" in codes["code"].to_list()

    def test_rerun_is_skipped_without_overwrite(self, tmp_path: Path, extraction_config: Path) -> None:
        project = run_extraction(tmp_path, extraction_config)
        output = project.datasets_path / "extraction" / "data" / "testdb" / "1.0" / "vitals" / "CHART.parquet"
        first_mtime = output.stat().st_mtime_ns

        step = ExtractionStep.load(project, extraction_config)
        step.run()
        assert output.stat().st_mtime_ns == first_mtime

    def test_missing_dataset_path_is_skipped(self, tmp_path: Path, table_config_dir: Path) -> None:
        config_file = tmp_path / "extraction.yml"
        config_file.write_text(
            f"""\
name: Extraction
version: 1.0.0

config_files:
  - path: {table_config_dir}

config:
  data: []
"""
        )

        project = OpenICUProject(tmp_path / "project")
        ExtractionStep.load(project, config_file).run()  # must not raise

        data_path = project.datasets_path / "extraction" / "data"
        assert list(data_path.rglob("*.parquet")) == []

    def test_missing_source_file_is_skipped(
        self, tmp_path: Path, data_dir: Path, table_config_dir: Path, extraction_config: Path
    ) -> None:
        (data_dir / "vitals.csv").unlink()

        project = OpenICUProject(tmp_path / "project")
        ExtractionStep.load(project, extraction_config).run()  # must not raise

        data_path = project.datasets_path / "extraction" / "data"
        # measurements still processed, vitals skipped
        assert (data_path / "testdb" / "1.0" / "measurements" / "WEIGHT.parquet").exists()
        assert not (data_path / "testdb" / "1.0" / "vitals" / "CHART.parquet").exists()

    def test_config_snapshot_saved_to_project(self, tmp_path: Path, extraction_config: Path) -> None:
        project = run_extraction(tmp_path, extraction_config)
        snapshot = project.configs_path / "dataset" / "testdb" / "1.0" / "vitals.yml"
        assert snapshot.exists()
