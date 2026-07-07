"""Tests for the continuous-time renal SOFA transformer.

Two layers:

- scoring unit tests: parametrized permutations of ``_compute`` on in-memory
  creatinine/urine frames (no pipeline, no files) covering the ricu
  thresholds, creatinine LOCF, the trailing urine window, and the
  missing-vs-recorded-zero distinction;
- one end-to-end test: extraction + simple creatinine/urine concepts +
  the complex ``sofa_renal`` concept over a synthetic dataset.
"""

from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from open_icu import ConceptStep, ExtractionStep, OpenICUProject
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.transformers import SofaRenalTransformer
from tests.steps.conftest import load_concept_config, load_extracation_config

T0 = datetime(2024, 1, 1, 0, 0)


@pytest.fixture
def concept() -> ConceptConfig:
    return ConceptConfig(
        name="sofa_renal",
        version="1.0.0",
        unit="points",
        extension_columns={"dataset": 'col("dataset")'},
    )


@pytest.fixture
def complex_config() -> ComplexDatasetConceptConfig:
    return ComplexDatasetConceptConfig(
        name="sofa_renal",
        version="1.0",
        dataset="testdb",
        concept_transformer="open_icu.steps.concept.transformers.sofa.SofaRenalTransformer",
        concepts=["creatinine", "urine_output"],
    )


@pytest.fixture
def transformer(concept: ConceptConfig, complex_config: ComplexDatasetConceptConfig) -> SofaRenalTransformer:
    return SofaRenalTransformer(concept, complex_config)


def frame(*rows: tuple[int, datetime, float | None]) -> pl.LazyFrame:
    """A minimal concept frame (subject_id, time, numeric_value)."""
    return pl.LazyFrame(
        {
            "subject_id": [r[0] for r in rows],
            "time": [r[1] for r in rows],
            "numeric_value": [r[2] for r in rows],
        },
        schema={"subject_id": pl.Int64, "time": pl.Datetime(time_unit="us"), "numeric_value": pl.Float32},
    )


def scores(transformer: SofaRenalTransformer, crea: pl.LazyFrame, urine: pl.LazyFrame) -> list:
    out = transformer.transform({"creatinine": crea, "urine_output": urine}).collect().sort("subject_id", "time")
    return out["numeric_value"].to_list()


class TestCreatinineScore:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param(0.9, 0.0, id="normal"),
            pytest.param(1.2, 1.0, id="boundary-1"),
            pytest.param(1.9, 1.0, id="tier-1"),
            pytest.param(2.0, 2.0, id="boundary-2"),
            pytest.param(3.4, 2.0, id="tier-2"),
            pytest.param(3.5, 3.0, id="boundary-3"),
            pytest.param(4.9, 3.0, id="tier-3"),
            pytest.param(5.0, 4.0, id="boundary-4"),
            pytest.param(7.2, 4.0, id="tier-4"),
        ],
    )
    def test_tiers(self, transformer: SofaRenalTransformer, value: float, expected: float) -> None:
        # no urine data -> urine contributes 0, score is creatinine-driven
        assert scores(transformer, frame((1, T0, value)), frame()) == [expected]


class TestUrineScore:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param(0.0, 4.0, id="recorded-anuria"),
            pytest.param(199.0, 4.0, id="under-200"),
            pytest.param(200.0, 3.0, id="boundary-500-tier"),
            pytest.param(499.0, 3.0, id="under-500"),
            pytest.param(500.0, 0.0, id="normal-output"),
            pytest.param(900.0, 0.0, id="high-output"),
        ],
    )
    def test_tiers(self, transformer: SofaRenalTransformer, value: float, expected: float) -> None:
        # no creatinine -> score is urine-driven (single record within the window)
        assert scores(transformer, frame(), frame((1, T0, value))) == [expected]


class TestCombination:
    @pytest.mark.parametrize(
        ("crea", "urine", "expected"),
        [
            pytest.param(2.0, 100.0, 4.0, id="urine-dominates"),
            pytest.param(5.0, 900.0, 4.0, id="creatinine-dominates"),
            pytest.param(1.3, 300.0, 3.0, id="equal-both-three-ish"),
            pytest.param(0.5, 900.0, 0.0, id="both-normal"),
        ],
    )
    def test_takes_max(self, transformer: SofaRenalTransformer, crea: float, urine: float, expected: float) -> None:
        # coincident creatinine + urine collapse to one evaluation
        assert scores(transformer, frame((1, T0, crea)), frame((1, T0, urine))) == [expected]


class TestContinuousTimeSemantics:
    def test_one_event_per_measurement(self, transformer: SofaRenalTransformer) -> None:
        crea = frame((1, T0, 2.0), (1, T0 + timedelta(hours=5), 2.0))
        urine = frame((1, T0 + timedelta(hours=2), 100.0))
        # three distinct measurement times -> three re-evaluations
        assert len(scores(transformer, crea, urine)) == 3

    def test_creatinine_carried_forward(self, transformer: SofaRenalTransformer) -> None:
        crea = frame((1, T0, 4.0))  # score 3, no new creatinine afterwards
        urine = frame((1, T0 + timedelta(hours=1), 900.0))  # score 0
        # second row (urine event) still sees creatinine 4.0 via LOCF -> 3
        assert scores(transformer, crea, urine) == [3.0, 3.0]

    def test_missing_urine_is_not_zero(self, transformer: SofaRenalTransformer) -> None:
        crea = frame((1, T0, 0.5), (1, T0 + timedelta(hours=3), 0.5))
        # no urine records at all -> urine never scores 4 (missing != anuria)
        assert scores(transformer, crea, frame()) == [0.0, 0.0]

    def test_recorded_zero_is_anuria(self, transformer: SofaRenalTransformer) -> None:
        assert scores(transformer, frame(), frame((1, T0, 0.0))) == [4.0]

    def test_urine_window_is_trailing_24h(self, transformer: SofaRenalTransformer) -> None:
        urine = frame((1, T0, 100.0))
        inside = frame((1, T0 + timedelta(hours=23), 0.5))  # 100 mL still in window -> 4
        outside = frame((1, T0 + timedelta(hours=25), 0.5))  # 100 mL expired -> urine 0
        assert scores(transformer, inside, urine) == [4.0, 4.0]
        assert scores(transformer, outside, urine) == [4.0, 0.0]

    def test_urine_accumulates_within_window(self, transformer: SofaRenalTransformer) -> None:
        # cumulative 24h urine: 150 (<200 -> 4), 300 (<500 -> 3), 450 (<500 -> 3)
        urine = frame(
            (1, T0, 150.0),
            (1, T0 + timedelta(hours=3), 150.0),
            (1, T0 + timedelta(hours=6), 150.0),
        )
        assert scores(transformer, frame(), urine) == [4.0, 3.0, 3.0]

    def test_subjects_are_independent(self, transformer: SofaRenalTransformer) -> None:
        crea = frame((1, T0, 5.5), (2, T0, 0.5))
        result = transformer.transform({"creatinine": crea, "urine_output": frame()}).collect().sort("subject_id")
        assert result["numeric_value"].to_list() == [4.0, 0.0]


LABS_CSV = """\
subject_id,charttime,itemid,valuenum
1,2024-01-01 00:00:00,CREA,2.0
1,2024-01-01 00:00:00,URINE,300
1,2024-01-01 02:00:00,CREA,5.5
"""

LABS_TABLE_YML = """\
path: labs.csv
columns:
  - name: subject_id
    type: int64
  - name: charttime
    type: datetime
    params:
      format: "%Y-%m-%d %H:%M:%S"
  - name: itemid
    type: string
  - name: valuenum
    type: float32

event_defaults:
  subject_id: col(subject_id)
  time: col(charttime)

events:
  - name: LAB
    columns:
      code:
        - col(itemid)
      numeric_value: col(valuenum)
"""

CREATININE_CONCEPT_YML = """\
name: creatinine
version: 1.0.0
unit: mg/dL
extension_columns:
  dataset: col("dataset")
  table: col("table")
"""

URINE_CONCEPT_YML = """\
name: urine_output
version: 1.0.0
unit: mL
extension_columns:
  dataset: col("dataset")
  table: col("table")
"""

SOFA_RENAL_CONCEPT_YML = """\
name: sofa_renal
version: 1.0.0
unit: points
extension_columns:
  dataset: col("dataset")
"""

CREATININE_MAPPING_YML = """\
type: simple
mappings:
  - pattern:
      table: labs
      event: LAB
      code: CREA
    columns:
      numeric_value: col(numeric_value)
"""

URINE_MAPPING_YML = """\
type: simple
mappings:
  - pattern:
      table: labs
      event: LAB
      code: URINE
    columns:
      numeric_value: col(numeric_value)
"""

SOFA_RENAL_MAPPING_YML = """\
type: complex
concept_transformer: open_icu.steps.concept.transformers.sofa.SofaRenalTransformer
concepts:
  - creatinine.1.0.0
  - urine_output.1.0.0
"""


def test_end_to_end_sofa_renal_concept(tmp_path: Path) -> None:
    """Full pipeline: extraction -> simple crea/urine concepts -> complex renal SOFA."""
    data_dir = tmp_path / "data" / "testdb"
    data_dir.mkdir(parents=True)
    (data_dir / "labs.csv").write_text(LABS_CSV)

    table_dir = tmp_path / "config" / "testdb" / "1.0" / "tables"
    table_dir.mkdir(parents=True)
    (table_dir / "labs.yml").write_text(LABS_TABLE_YML)

    concept_dir = tmp_path / "config" / "concepts"
    concept_dir.mkdir(parents=True)
    (concept_dir / "creatinine.yml").write_text(CREATININE_CONCEPT_YML)
    (concept_dir / "urine_output.yml").write_text(URINE_CONCEPT_YML)
    (concept_dir / "sofa_renal.yml").write_text(SOFA_RENAL_CONCEPT_YML)

    mapping_dir = tmp_path / "config" / "testdb" / "1.0" / "mappings"
    mapping_dir.mkdir(parents=True)
    (mapping_dir / "creatinine.yml").write_text(CREATININE_MAPPING_YML)
    (mapping_dir / "urine_output.yml").write_text(URINE_MAPPING_YML)
    (mapping_dir / "sofa_renal.yml").write_text(SOFA_RENAL_MAPPING_YML)

    (tmp_path / "extraction.yml").write_text(
        f"""\
name: Extraction
version: 1.0.0

config:
  data:
    - name: testdb
      version: "1.0"
      path: {data_dir}
"""
    )
    (tmp_path / "concept.yml").write_text(
        """\
name: Concept
version: 1.0.0

config:
  extraction_step: Extraction
  mapping_configs:
    - name: testdb
      version: "1.0"
"""
    )

    project = OpenICUProject(tmp_path / "project")
    load_extracation_config(table_dir)
    load_concept_config(concept_dir, [mapping_dir])
    ExtractionStep.load(project, tmp_path / "extraction.yml").run()
    ConceptStep.load(project, tmp_path / "concept.yml").run()

    df = pl.read_parquet(project.datasets_path / "concept" / "data" / "sofa_renal" / "1.0.0" / "testdb.parquet").sort(
        "time"
    )

    # 00:00 -> crea 2.0 (2) vs urine24 300 (<500 -> 3) -> 3
    # 02:00 -> crea 5.5 (4) vs urine24 300 (3)          -> 4
    assert df["code"].to_list() == ["sofa_renal//points", "sofa_renal//points"]
    assert df["numeric_value"].to_list() == [3.0, 4.0]
    assert df.schema["numeric_value"] == pl.Float32
    assert df["dataset"].unique().to_list() == ["testdb"]

    codes = pl.read_parquet(project.datasets_path / "concept" / "metadata" / "codes.parquet")
    assert "sofa_renal//points" in codes["code"].to_list()
