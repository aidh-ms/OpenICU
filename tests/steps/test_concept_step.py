"""End-to-end tests for the concept step on synthetic fixture data."""

from pathlib import Path

import polars as pl
import pytest

from open_icu import ConceptStep, ExtractionStep, OpenICUProject


@pytest.fixture
def project(tmp_path: Path, extraction_config: Path, concept_config: Path) -> OpenICUProject:
    project = OpenICUProject(tmp_path / "project")
    ExtractionStep.load(project, extraction_config).run()
    ConceptStep.load(project, concept_config).run()
    return project


def concept_path(project: OpenICUProject, name: str) -> Path:
    return project.datasets_path / "concept" / "data" / name / "1.0.0" / "testdb.parquet"


class TestSimpleConcepts:
    def test_simple_concept_selects_and_recodes(self, project: OpenICUProject) -> None:
        df = pl.read_parquet(concept_path(project, "heart_rate")).sort("time")

        assert df.height == 2  # only the two heart rate rows match
        assert df["code"].unique().to_list() == ["heart_rate//bpm"]
        assert df["numeric_value"].to_list() == [80.0, 82.0]
        assert df["text_value"].to_list() == ["eighty", None]

    def test_meds_schema(self, project: OpenICUProject) -> None:
        df = pl.read_parquet(concept_path(project, "heart_rate"))
        assert df.schema["subject_id"] == pl.Int64
        assert df.schema["time"] == pl.Datetime(time_unit="us")
        assert df.schema["code"] == pl.String
        assert df.schema["numeric_value"] == pl.Float32
        assert df.schema["text_value"] == pl.String

    def test_provenance_extension_columns(self, project: OpenICUProject) -> None:
        df = pl.read_parquet(concept_path(project, "heart_rate"))
        assert df["dataset"].unique().to_list() == ["testdb"]
        assert df["table"].unique().to_list() == ["vitals"]

    def test_unit_concepts(self, project: OpenICUProject) -> None:
        weight = pl.read_parquet(concept_path(project, "patient_weight"))
        assert weight["code"].unique().to_list() == ["patient_weight//kg"]
        assert sorted(weight["numeric_value"].to_list()) == [60.0, 80.0]


class TestDerivedConcepts:
    def test_derived_concept_computed_from_dependencies(self, project: OpenICUProject) -> None:
        df = pl.read_parquet(concept_path(project, "bmi")).sort("subject_id")

        assert df["code"].unique().to_list() == ["bmi//kg/m2"]
        # subject 1: 80 kg / (2.0 m)^2 = 20; subject 2: 60 kg / (1.5 m)^2 = 26.67
        assert df["numeric_value"].to_list() == pytest.approx([20.0, 26.666666], abs=1e-4)

    def test_codes_metadata_contains_all_concepts(self, project: OpenICUProject) -> None:
        codes = pl.read_parquet(project.datasets_path / "concept" / "metadata" / "codes.parquet")
        code_list = codes["code"].to_list()
        for expected in ["heart_rate//bpm", "patient_weight//kg", "patient_height//m", "bmi//kg/m2"]:
            assert expected in code_list


class TestConceptStepRobustness:
    def test_concept_without_dataset_mapping_is_skipped(
        self, tmp_path: Path, extraction_config: Path, concept_config: Path
    ) -> None:
        # Add a concept that has no mapping for testdb.
        (tmp_path / "config" / "concept" / "orphan.yml").write_text(
            "name: orphan\nversion: 1.0.0\nunit: x\n"
        )

        project = OpenICUProject(tmp_path / "project")
        ExtractionStep.load(project, extraction_config).run()
        ConceptStep.load(project, concept_config).run()  # must not raise

        assert not (project.datasets_path / "concept" / "data" / "orphan").exists()

    def test_missing_extraction_event_is_skipped(
        self, tmp_path: Path, extraction_config: Path, concept_config: Path
    ) -> None:
        # Point a mapping at an event that does not exist.
        mapping_dir = tmp_path / "config" / "testdb" / "1.0" / "concept"
        (mapping_dir / "heart_rate.yml").write_text(
            """\
type: simple
mappings:
  - pattern:
      table: vitals
      event: DOES_NOT_EXIST
      code: (220045//Heart Rate)
    columns:
      numeric_value: col(numeric_value)
"""
        )

        project = OpenICUProject(tmp_path / "project")
        ExtractionStep.load(project, extraction_config).run()
        ConceptStep.load(project, concept_config).run()  # must not raise

        assert not concept_path(project, "heart_rate").exists()
