"""Scoring unit tests for the remaining SOFA components and the total.

Each component's ``score`` is checked against ricu's thresholds on in-memory
frames; the total is checked as a LOCF sum that re-evaluates whenever any
component changes. Alignment/windowing itself is covered in
``test_windowed_transformer.py`` and ``test_sofa_renal_transformer.py``.
"""

from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from open_icu import ConceptStep, ExtractionStep, OpenICUProject
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.transformers import (
    SofaCardiovascularTransformer,
    SofaCnsTransformer,
    SofaCoagulationTransformer,
    SofaLiverTransformer,
    SofaRespirationTransformer,
    WindowedSumTransformer,
)
from open_icu.steps.concept.transformers.windowed import WindowedConceptTransformer
from tests.steps.conftest import load_concept_config, load_extracation_config

T0 = datetime(2024, 1, 1, 0, 0)


def make(cls: type[WindowedConceptTransformer], **kwargs) -> WindowedConceptTransformer:
    concept = ConceptConfig(name="component", version="1.0.0", unit="points")
    config = ComplexDatasetConceptConfig(
        name="component", version="1.0", dataset="testdb", concept_transformer="unused"
    )
    return cls(concept, config, **kwargs)


def frame(*rows: tuple[int, datetime, float | None]) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "subject_id": [r[0] for r in rows],
            "time": [r[1] for r in rows],
            "numeric_value": [r[2] for r in rows],
        },
        schema={"subject_id": pl.Int64, "time": pl.Datetime(time_unit="us"), "numeric_value": pl.Float32},
    )


def score(transformer: WindowedConceptTransformer, **values: float) -> list:
    """Score a single instant with the named inputs measured at T0."""
    inputs = {name: frame((1, T0, value)) for name, value in values.items()}
    return transformer.transform(inputs).collect()["numeric_value"].to_list()


@pytest.mark.parametrize(
    ("platelets", "expected"),
    [(10, 4.0), (20, 3.0), (40, 3.0), (50, 2.0), (80, 2.0), (100, 1.0), (140, 1.0), (150, 0.0), (300, 0.0)],
)
def test_coagulation(platelets: float, expected: float) -> None:
    assert score(make(SofaCoagulationTransformer), platelet_count=platelets) == [expected]


@pytest.mark.parametrize(
    ("bilirubin", "expected"),
    [(0.5, 0.0), (1.2, 1.0), (1.9, 1.0), (2.0, 2.0), (6.0, 3.0), (11.9, 3.0), (12.0, 4.0)],
)
def test_liver(bilirubin: float, expected: float) -> None:
    assert score(make(SofaLiverTransformer), total_bilirubin=bilirubin) == [expected]


@pytest.mark.parametrize(
    ("gcs", "expected"),
    [(15, 0.0), (14, 1.0), (13, 1.0), (12, 2.0), (10, 2.0), (9, 3.0), (6, 3.0), (5, 4.0), (3, 4.0)],
)
def test_cns(gcs: float, expected: float) -> None:
    assert score(make(SofaCnsTransformer), GCS_total=gcs) == [expected]


class TestCardiovascular:
    @pytest.mark.parametrize(
        ("field", "value", "expected"),
        [
            ("dopamine_rate", 20.0, 4.0),
            ("epinephrine_rate", 0.2, 4.0),
            ("norepinephrine_rate", 0.2, 4.0),
            ("dopamine_rate", 8.0, 3.0),
            ("epinephrine_rate", 0.05, 3.0),
            ("norepinephrine_rate", 0.05, 3.0),
            ("dopamine_rate", 3.0, 2.0),
            ("dobutamine_rate", 2.0, 2.0),
        ],
    )
    def test_vasopressor_tiers(self, field: str, value: float, expected: float) -> None:
        assert score(make(SofaCardiovascularTransformer), **{field: value}) == [expected]

    @pytest.mark.parametrize(("map_value", "expected"), [(60.0, 1.0), (70.0, 0.0), (90.0, 0.0)])
    def test_map_without_pressors(self, map_value: float, expected: float) -> None:
        assert score(make(SofaCardiovascularTransformer), mean_arterial_pressure=map_value) == [expected]

    def test_highest_qualifying_tier_wins(self) -> None:
        # low MAP but also high-dose norepinephrine -> the vasopressor tier dominates
        transformer = make(SofaCardiovascularTransformer)
        assert score(transformer, mean_arterial_pressure=50.0, norepinephrine_rate=0.3) == [4.0]


class TestRespiration:
    @pytest.mark.parametrize(
        ("pao2", "fio2", "vent", "expected"),
        [
            (60.0, 100.0, 1.0, 4.0),  # pafi 60, ventilated
            (150.0, 100.0, 1.0, 3.0),  # pafi 150, ventilated
            (250.0, 100.0, 1.0, 2.0),  # pafi 250
            (350.0, 100.0, 1.0, 1.0),  # pafi 350
            (500.0, 100.0, 1.0, 0.0),  # pafi 500
            (60.0, 100.0, 0.0, 2.0),  # pafi 60 but NOT ventilated -> caps at tier 2
        ],
    )
    def test_pafi_and_ventilation(self, pao2: float, fio2: float, vent: float, expected: float) -> None:
        transformer = make(SofaRespirationTransformer)
        result = score(
            transformer,
            O2_partial_pressure=pao2,
            fraction_of_inspired_oxygen=fio2,
            mechanical_ventilation_windows=vent,
        )
        assert result == [expected]

    def test_unknown_ventilation_degrades_to_lower_tiers(self) -> None:
        # pafi 60 with no ventilation record -> cannot reach tier 3/4, scores 2
        transformer = make(SofaRespirationTransformer)
        assert score(transformer, O2_partial_pressure=60.0, fraction_of_inspired_oxygen=100.0) == [2.0]

    def test_zero_fio2_yields_no_ratio(self) -> None:
        transformer = make(SofaRespirationTransformer)
        assert score(transformer, O2_partial_pressure=90.0, fraction_of_inspired_oxygen=0.0) == [0.0]


def test_total_sofa_is_locf_sum_of_components() -> None:
    total = make(WindowedSumTransformer, terms=["sofa_renal", "sofa_cns"])
    renal = frame((1, T0, 2.0))
    cns = frame((1, T0 + timedelta(hours=1), 1.0))
    out = total.transform({"sofa_renal": renal, "sofa_cns": cns}).collect().sort("time")
    # T0: renal 2 (cns not yet measured -> 0); T0+1h: renal 2 (LOCF) + cns 1
    assert out["numeric_value"].to_list() == [2.0, 3.0]


# --- end-to-end: the deepest dependency chain --------------------------------
# GCS parts -> GCS_total (sum) -> sofa_cns, platelets -> sofa_coagulation, and
# sofa (total) -> both components. Exercises complex-depends-on-complex ordering
# and cross-timestamp alignment through the real ConceptStep.

LABS_CSV = """\
subject_id,charttime,itemid,valuenum
1,2024-01-01 00:00:00,EYE,1
1,2024-01-01 00:00:00,MOTOR,1
1,2024-01-01 00:00:00,VERBAL,1
1,2024-01-01 00:00:00,PLT,30
1,2024-01-01 01:00:00,PLT,200
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


def _concept_yml(name: str, unit: str) -> str:
    return f'name: {name}\nversion: 1.0.0\nunit: {unit}\nextension_columns:\n  dataset: col("dataset")\n'


def _simple_mapping_yml(code: str) -> str:
    return (
        "type: simple\n"
        "mappings:\n"
        "  - pattern:\n"
        "      table: labs\n"
        "      event: LAB\n"
        f"      code: {code}\n"
        "    columns:\n"
        "      numeric_value: col(numeric_value)\n"
    )


def _sum_mapping_yml(transformer: str, terms: list[str]) -> str:
    versioned = "".join(f"  - {t}.1.0.0\n" for t in terms)
    named = "".join(f"    - {t}\n" for t in terms)
    return f"type: complex\nconcept_transformer: {transformer}\nconcepts:\n{versioned}kwargs:\n  terms:\n{named}"


def _component_mapping_yml(transformer: str, inputs: list[str]) -> str:
    versioned = "".join(f"  - {i}.1.0.0\n" for i in inputs)
    return f"type: complex\nconcept_transformer: {transformer}\nconcepts:\n{versioned}"


def test_end_to_end_total_sofa_chain(tmp_path: Path) -> None:
    win_sum = "open_icu.steps.concept.transformers.windowed.WindowedSumTransformer"
    sofa_cns = "open_icu.steps.concept.transformers.sofa.SofaCnsTransformer"
    sofa_coag = "open_icu.steps.concept.transformers.sofa.SofaCoagulationTransformer"

    data_dir = tmp_path / "data" / "testdb"
    data_dir.mkdir(parents=True)
    (data_dir / "labs.csv").write_text(LABS_CSV)

    table_dir = tmp_path / "config" / "testdb" / "1.0" / "tables"
    table_dir.mkdir(parents=True)
    (table_dir / "labs.yml").write_text(LABS_TABLE_YML)

    concept_dir = tmp_path / "config" / "concepts"
    concept_dir.mkdir(parents=True)
    for name, unit in [
        ("GCS_eye", "points"),
        ("GCS_motor", "points"),
        ("GCS_verbal", "points"),
        ("GCS_total", "points"),
        ("platelet_count", "K/uL"),
        ("sofa_cns", "points"),
        ("sofa_coagulation", "points"),
        ("sofa", "points"),
    ]:
        (concept_dir / f"{name}.yml").write_text(_concept_yml(name, unit))

    mapping_dir = tmp_path / "config" / "testdb" / "1.0" / "mappings"
    mapping_dir.mkdir(parents=True)
    (mapping_dir / "GCS_eye.yml").write_text(_simple_mapping_yml("EYE"))
    (mapping_dir / "GCS_motor.yml").write_text(_simple_mapping_yml("MOTOR"))
    (mapping_dir / "GCS_verbal.yml").write_text(_simple_mapping_yml("VERBAL"))
    (mapping_dir / "platelet_count.yml").write_text(_simple_mapping_yml("PLT"))
    (mapping_dir / "GCS_total.yml").write_text(_sum_mapping_yml(win_sum, ["GCS_eye", "GCS_motor", "GCS_verbal"]))
    (mapping_dir / "sofa_cns.yml").write_text(_component_mapping_yml(sofa_cns, ["GCS_total"]))
    (mapping_dir / "sofa_coagulation.yml").write_text(_component_mapping_yml(sofa_coag, ["platelet_count"]))
    (mapping_dir / "sofa.yml").write_text(_sum_mapping_yml(win_sum, ["sofa_cns", "sofa_coagulation"]))

    (tmp_path / "extraction.yml").write_text(
        f'name: Extraction\nversion: 1.0.0\n\nconfig:\n  data:\n    - name: testdb\n      version: "1.0"\n      path: {data_dir}\n'
    )
    (tmp_path / "concept.yml").write_text(
        "name: Concept\nversion: 1.0.0\n\nconfig:\n"
        "  extraction_step: Extraction\n"
        '  mapping_configs:\n    - name: testdb\n      version: "1.0"\n'
    )

    project = OpenICUProject(tmp_path / "project")
    load_extracation_config(table_dir)
    load_concept_config(concept_dir, [mapping_dir])
    ExtractionStep.load(project, tmp_path / "extraction.yml").run()
    ConceptStep.load(project, tmp_path / "concept.yml").run()

    sofa = pl.read_parquet(project.datasets_path / "concept" / "data" / "sofa" / "1.0.0" / "testdb.parquet").sort(
        "time"
    )

    # GCS 1+1+1=3 -> CNS 4; PLT 30 -> coag 3; total 7 at 00:00.
    # At 01:00 PLT 200 -> coag 0, CNS 4 carried forward -> total 4.
    assert sofa["code"].to_list() == ["sofa//points", "sofa//points"]
    assert sofa["numeric_value"].to_list() == [7.0, 4.0]
