"""Tests for the ICD-9-CM -> ICD-10-CM diagnosis harmonisation transformer.

Three layers:

- mapping-policy unit tests: parametrized permutations of `_harmonise` on
  in-memory frames against a stubbed GEM lookup (no pipeline, no bundled data)
- data-integrity tests: properties and spot checks of the bundled CMS GEM
- one end-to-end test: full extraction + concept step run over a synthetic
  dataset with the real crosswalk
"""
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from open_icu import ConceptStep, ExtractionStep, OpenICUProject
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.transformers import ICD9ToICD10Transformer, load_gem_lookup
from tests.steps.conftest import load_concept_config, load_extracation_config

CODE_PATTERN = "^[^/]+//diagnoses//ICD//(?<icd_version>[^/]+)//(?<icd_code>[^/]+)$"

STUB_GEM = pl.DataFrame(
    {
        "icd9cm": ["4019", "00589", "E8790"],
        "icd10cm": ["I10", "A054", "Y840"],
    }
)


@pytest.fixture
def concept() -> ConceptConfig:
    return ConceptConfig(
        name="diagnosis",
        version="1.0.0",
        unit="ICD10CM",
        extension_columns={"dataset": 'col("dataset")', "table": 'col("table")'},
    )


@pytest.fixture
def complex_config() -> ComplexDatasetConceptConfig:
    return ComplexDatasetConceptConfig(
        name="diagnosis",
        version="1.0",
        dataset="testdb",
        concept_transformer="open_icu.steps.concept.transformers.icd.ICD9ToICD10Transformer",
    )


@pytest.fixture
def transformer(
    concept: ConceptConfig,
    complex_config: ComplexDatasetConceptConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> ICD9ToICD10Transformer:
    """Transformer wired to the stub GEM (load_gem_lookup is lru_cached, so patch the module attribute)."""
    monkeypatch.setattr("open_icu.steps.concept.transformers.icd.load_gem_lookup", lambda: STUB_GEM)
    return ICD9ToICD10Transformer(
        concept,
        complex_config,
        table="diagnoses",
        event="DIAGNOSIS",
        code_pattern=CODE_PATTERN,
    )


def make_events(*codes: str) -> pl.LazyFrame:
    """A minimal extraction-output frame with the standard MEDS columns."""
    return pl.LazyFrame(
        {
            "subject_id": [1] * len(codes),
            "time": [datetime(2024, 1, 5, 12)] * len(codes),
            "code": list(codes),
            "numeric_value": [None] * len(codes),
            "text_value": [None] * len(codes),
        },
        schema_overrides={"numeric_value": pl.Float32, "text_value": pl.String},
    )


def diagnosis_code(version: str, icd_code: str) -> str:
    """An extraction code as the DIAGNOSIS event assembles it."""
    return f"testdb//diagnoses//ICD//{version}//{icd_code}"


class TestMappingPolicy:
    @pytest.mark.parametrize(
        ("version", "icd_code", "expected"),
        [
            pytest.param("10", "I214", "diagnosis//ICD10CM//I214", id="icd10-passthrough"),
            pytest.param("10", "I21.4", "diagnosis//ICD10CM//I214", id="icd10-dot-stripped"),
            pytest.param("9", "4019", "diagnosis//ICD10CM//I10", id="icd9-gem-hit"),
            pytest.param("9", "401.9", "diagnosis//ICD10CM//I10", id="icd9-dotted-normalised"),
            pytest.param("9", "e879.0", "diagnosis//ICD10CM//Y840", id="icd9-uppercased"),
            pytest.param("9", "36570", "diagnosis//ICD9CM//36570", id="icd9-no-map-kept"),
        ],
    )
    def test_code_harmonisation(
        self, transformer: ICD9ToICD10Transformer, version: str, icd_code: str, expected: str
    ) -> None:
        df = transformer._harmonise(make_events(diagnosis_code(version, icd_code)), "DIAGNOSIS").collect()

        assert df["code"].to_list() == [expected]
        assert df["icd_code"].to_list() == [icd_code]  # source code preserved verbatim
        assert df["icd_version"].to_list() == [version]

    @pytest.mark.parametrize(
        "code",
        [
            pytest.param(diagnosis_code("11", "4019"), id="unknown-icd-version"),
            pytest.param("not-a-diagnosis-code", id="pattern-mismatch"),
        ],
    )
    def test_unparseable_rows_are_dropped(self, transformer: ICD9ToICD10Transformer, code: str) -> None:
        assert transformer._harmonise(make_events(code), "DIAGNOSIS").collect().height == 0

    def test_output_schema_and_provenance(self, transformer: ICD9ToICD10Transformer) -> None:
        df = transformer._harmonise(
            make_events(diagnosis_code("9", "4019"), diagnosis_code("10", "I214")), "DIAGNOSIS"
        ).collect()

        assert df.schema == pl.Schema(
            {
                "subject_id": pl.Int64,
                "time": pl.Datetime(time_unit="us"),
                "code": pl.String,
                "numeric_value": pl.Float32,
                "text_value": pl.String,
                "icd_code": pl.String,
                "icd_version": pl.String,
                "dataset": pl.String,
                "table": pl.String,
            }
        )
        assert df["dataset"].unique().to_list() == ["testdb"]
        assert df["table"].unique().to_list() == ["diagnoses"]


class TestBundledGem:
    def test_lookup_is_one_to_one(self) -> None:
        gem = load_gem_lookup()
        assert gem.columns == ["icd9cm", "icd10cm"]
        assert gem["icd9cm"].n_unique() == gem.height

    @pytest.mark.parametrize(
        ("icd9", "icd10"),
        [
            pytest.param("4019", "I10", id="exact-single-match"),
            pytest.param("00589", "A054", id="multi-target-first-gem-row"),
            pytest.param("V090", "Z1611", id="supplementary-v-code"),
            pytest.param("E8790", "Y840", id="supplementary-e-code"),
        ],
    )
    def test_known_mappings(self, icd9: str, icd10: str) -> None:
        gem = load_gem_lookup()
        assert gem.filter(pl.col("icd9cm") == icd9)["icd10cm"].to_list() == [icd10]

    def test_no_map_sources_are_dropped(self) -> None:
        gem = load_gem_lookup()
        assert gem.filter(pl.col("icd9cm") == "36570").is_empty()
        assert not (gem["icd10cm"] == "NoDx").any()


DIAGNOSES_CSV = """\
subject_id,dischtime,icd_code,icd_version
1,2024-01-05 12:00:00,4019,9
1,2024-01-05 12:00:00,I21.4,10
2,2024-02-01 08:00:00,36570,9
"""

DIAGNOSES_TABLE_YML = """\
path: diagnoses.csv
columns:
  - name: subject_id
    type: int64
  - name: dischtime
    type: datetime
    params:
      format: "%Y-%m-%d %H:%M:%S"
  - name: icd_code
    type: string
  - name: icd_version
    type: int64

event_defaults:
  subject_id: col(subject_id)
  time: col(dischtime)

events:
  - name: DIAGNOSIS
    columns:
      code:
        - const(ICD)
        - col(icd_version)
        - col(icd_code)
"""

DIAGNOSIS_CONCEPT_YML = """\
name: diagnosis
version: 1.0.0
unit: ICD10CM
extension_columns:
  dataset: col("dataset")
  table: col("table")
"""

DIAGNOSIS_MAPPING_YML = f"""\
type: complex
concept_transformer: open_icu.steps.concept.transformers.icd.ICD9ToICD10Transformer
kwargs:
  table: diagnoses
  event: DIAGNOSIS
  code_pattern: '{CODE_PATTERN}'
"""


def test_end_to_end_diagnosis_concept(tmp_path: Path) -> None:
    """Full pipeline with the real GEM: extraction -> complex concept -> MEDS dataset."""
    data_dir = tmp_path / "data" / "testdb"
    data_dir.mkdir(parents=True)
    (data_dir / "diagnoses.csv").write_text(DIAGNOSES_CSV)

    table_dir = tmp_path / "config" / "testdb" / "1.0" / "tables"
    table_dir.mkdir(parents=True)
    (table_dir / "diagnoses.yml").write_text(DIAGNOSES_TABLE_YML)

    concept_dir = tmp_path / "config" / "concepts"
    concept_dir.mkdir(parents=True)
    (concept_dir / "diagnosis.yml").write_text(DIAGNOSIS_CONCEPT_YML)

    mapping_dir = tmp_path / "config" / "testdb" / "1.0" / "mappings"
    mapping_dir.mkdir(parents=True)
    (mapping_dir / "diagnosis.yml").write_text(DIAGNOSIS_MAPPING_YML)

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

    df = pl.read_parquet(
        project.datasets_path / "concept" / "data" / "diagnosis" / "1.0.0" / "testdb.parquet"
    ).sort("icd_code")

    assert df["code"].to_list() == [
        "diagnosis//ICD9CM//36570",  # no_map: kept, not dropped
        "diagnosis//ICD10CM//I10",  # GEM-mapped ICD-9
        "diagnosis//ICD10CM//I214",  # native ICD-10
    ]
    assert df.schema["numeric_value"] == pl.Float32

    codes = pl.read_parquet(project.datasets_path / "concept" / "metadata" / "codes.parquet")
    assert "diagnosis//ICD10CM//I10" in codes["code"].to_list()
