"""Tests for the SOFA transformers: every organ sub-score and the total.

Three layers:

- scoring unit tests, checking each component against ricu's thresholds on
  in-memory frames (no pipeline, no files);
- continuous-time semantics, concentrated on the renal component because it
  is the one with two inputs on different aggregations — an expiring
  creatinine carry-forward and a segmented urine window — and so exercises
  alignment as well as scoring;
- two end-to-end tests through the real pipeline: the renal component over
  creatinine and urine, and the deepest dependency chain (GCS parts ->
  GCS_total -> sofa_cns, platelets -> sofa_coagulation, both -> sofa).

The general windowing machinery itself is covered in
``test_windowed_transformer.py``.

Note on the urine criterion: a trailing sum is reported only across a window
that was observed end to end, so a lone urine record does not score. The
fixtures below therefore open a segment 24h before the instant under test.
That gating is the point of the criterion — an undocumented window must not
read as oliguria — so it is exercised rather than worked around.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

import polars as pl
import pytest

from open_icu import ConceptStep, ExtractionStep, OpenICUProject
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.transformer.sofa import (
    SofaCardiovascularTransformer,
    SofaCnsTransformer,
    SofaCoagulationTransformer,
    SofaLiverTransformer,
    SofaRenalTransformer,
    SofaRespiratoryTransformer,
    SofaTransformer,
)
from open_icu.steps.concept.transformer.windowed import WindowedConceptTransformer
from tests.steps.conftest import load_concept_config, load_extracation_config

T0 = datetime(2024, 1, 1, 0, 0)
DATASET = "testdb"


def make(cls: type[WindowedConceptTransformer], **kwargs) -> WindowedConceptTransformer:
    """Build a transformer for ``transform``-only testing.

    ``transform`` is pure and never touches the step, so none is needed.
    """
    concept = ConceptConfig(name="component", version="1.0.0", unit="points")
    config = ComplexDatasetConceptConfig(
        name="component", version="1.0", dataset=DATASET, concept_transformer="unused"
    )
    return cls(concept, config, cast(ConceptStep, None), **kwargs)


@pytest.fixture
def renal() -> SofaRenalTransformer:
    return cast(SofaRenalTransformer, make(SofaRenalTransformer))


def at(hours: float) -> datetime:
    return T0 + timedelta(hours=hours)


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


def covered_urine(*rows: tuple[float, float]) -> pl.LazyFrame:
    """Urine records opening a segment at T0, so windows from T0+24h are covered.

    The opening record is 0 mL and falls outside the trailing window at every
    instant tested, so it contributes to coverage without contributing volume.
    """
    return frame((1, T0, 0.0), *[(1, at(hours), volume) for hours, volume in rows])


def scores(transformer: WindowedConceptTransformer, inputs: dict[str, pl.LazyFrame]) -> list:
    """Every score the transformer emits, in time order."""
    out = transformer.transform(inputs).collect().sort("subject_id", "time")
    return out["numeric_value"].to_list()


def score(transformer: WindowedConceptTransformer, **values: float) -> list:
    """Score a single instant with the named inputs measured at T0."""
    return scores(transformer, {name: frame((1, T0, value)) for name, value in values.items()})


def renal_scores(transformer: SofaRenalTransformer, crea: pl.LazyFrame, urine: pl.LazyFrame) -> list:
    return scores(transformer, {"creatinine": crea, "urine_output": urine})


# --- scoring: one component at a time ----------------------------------------


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


def test_a_component_with_no_current_input_emits_nothing() -> None:
    """The zero that a total sums must come from a measurement, not from silence."""
    transformer = make(SofaCoagulationTransformer, window="24h")
    out = transformer.transform(
        {"platelet_count": frame((1, T0, 300.0)), "unused": frame((1, at(48), 1.0))}
    ).collect()
    # the platelet count has expired by the second event, and the component
    # declares no other input, so only its own measurement is emitted
    assert out.height == 1


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

    def test_a_stopped_infusion_must_be_charted_as_zero(self) -> None:
        # a rate of 0 is a measurement and scores 0; the tier is not taken
        assert score(make(SofaCardiovascularTransformer), norepinephrine_rate=0.0) == [0.0]


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
        result = score(
            make(SofaRespiratoryTransformer),
            O2_partial_pressure=pao2,
            fraction_of_inspired_oxygen=fio2,
            mechanical_ventilation_windows=vent,
        )
        assert result == [expected]

    def test_unknown_ventilation_degrades_to_lower_tiers(self) -> None:
        # pafi 60 with no ventilation record -> cannot reach tier 3/4, scores 2
        transformer = make(SofaRespiratoryTransformer)
        assert score(transformer, O2_partial_pressure=60.0, fraction_of_inspired_oxygen=100.0) == [2.0]

    def test_zero_fio2_yields_no_ratio_and_no_event(self) -> None:
        # without a ratio the component is not evaluable, so it emits nothing
        transformer = make(SofaRespiratoryTransformer)
        assert score(transformer, O2_partial_pressure=90.0, fraction_of_inspired_oxygen=0.0) == []

    def test_one_gas_alone_yields_no_event(self) -> None:
        assert score(make(SofaRespiratoryTransformer), fraction_of_inspired_oxygen=50.0) == []

    def _late_gases(self, **kwargs) -> list:
        transformer = make(SofaRespiratoryTransformer, window="24h", **kwargs)
        return scores(
            transformer,
            {
                "mechanical_ventilation_windows": frame((1, T0, 1.0)),
                "O2_partial_pressure": frame((1, at(72), 60.0)),
                "fraction_of_inspired_oxygen": frame((1, at(72), 100.0)),
            },
        )

    def test_ventilation_persists_without_re_charting(self) -> None:
        # ventilation is a state, carried forward past the measurement window
        assert self._late_gases() == [4.0]

    def test_ventilation_can_be_made_to_expire(self) -> None:
        # ...unless the mapping asks for it to expire, which degrades the score
        # to the ventilation-independent tiers
        assert self._late_gases(ventilation_window="24h") == [2.0]


# --- renal: scoring and continuous-time semantics -----------------------------


class TestRenalCreatinineScore:
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
    def test_tiers(self, renal: SofaRenalTransformer, value: float, expected: float) -> None:
        # no urine data -> the urine criterion is not gradeable, score is creatinine-driven
        assert renal_scores(renal, frame((1, T0, value)), frame()) == [expected]


class TestRenalUrineScore:
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
    def test_tiers(self, renal: SofaRenalTransformer, value: float, expected: float) -> None:
        # no creatinine -> score is urine-driven, over a fully covered 24h window
        assert renal_scores(renal, frame(), covered_urine((24, value))) == [expected]

    def test_partial_window_is_not_scored(self, renal: SofaRenalTransformer) -> None:
        # the same low volume, but only 6h of the window observed -> no event at all
        assert renal_scores(renal, frame(), frame((1, T0, 0.0), (1, at(6), 100.0))) == []


class TestRenalCombination:
    @pytest.mark.parametrize(
        ("crea", "urine", "expected"),
        [
            pytest.param(2.0, 100.0, 4.0, id="urine-dominates"),
            pytest.param(5.0, 900.0, 4.0, id="creatinine-dominates"),
            pytest.param(1.3, 300.0, 3.0, id="equal-both-three-ish"),
            pytest.param(0.5, 900.0, 0.0, id="both-normal"),
        ],
    )
    def test_takes_max(self, renal: SofaRenalTransformer, crea: float, urine: float, expected: float) -> None:
        # coincident creatinine + urine collapse to one evaluation
        assert renal_scores(renal, frame((1, at(24), crea)), covered_urine((24, urine))) == [expected]


class TestContinuousTimeSemantics:
    def test_one_event_per_measurement(self, renal: SofaRenalTransformer) -> None:
        crea = frame((1, T0, 2.0), (1, at(5), 2.0))
        # three distinct measurement times -> three re-evaluations
        assert len(renal_scores(renal, crea, frame((1, at(2), 100.0)))) == 3

    def test_creatinine_carried_forward(self, renal: SofaRenalTransformer) -> None:
        crea = frame((1, T0, 4.0))  # score 3, no new creatinine afterwards
        urine = frame((1, at(1), 900.0))  # not yet a gradeable window
        # second row (urine event) still sees creatinine 4.0 via LOCF -> 3
        assert renal_scores(renal, crea, urine) == [3.0, 3.0]

    def test_creatinine_expires_after_the_window(self, renal: SofaRenalTransformer) -> None:
        crea = frame((1, T0, 4.0))
        # at 24h the creatinine is still current; at 25h it is not, and the urine
        # criterion cannot grade either, so nothing is emitted
        assert renal_scores(renal, crea, frame((1, at(24), 900.0))) == [3.0, 3.0]
        assert renal_scores(renal, crea, frame((1, at(25), 900.0))) == [3.0]

    def test_missing_urine_is_not_zero(self, renal: SofaRenalTransformer) -> None:
        crea = frame((1, T0, 0.5), (1, at(3), 0.5))
        # no urine records at all -> urine never scores 4 (missing != anuria)
        assert renal_scores(renal, crea, frame()) == [0.0, 0.0]

    def test_recorded_zero_across_a_covered_window_is_anuria(self, renal: SofaRenalTransformer) -> None:
        assert renal_scores(renal, frame(), covered_urine((12, 0.0), (24, 0.0))) == [4.0]

    def test_urine_accumulates_within_the_window(self, renal: SofaRenalTransformer) -> None:
        # cumulative 24h urine: 150 (<200 -> 4), 300 (<500 -> 3), 450 (<500 -> 3)
        assert renal_scores(renal, frame(), covered_urine((24, 150.0), (27, 150.0), (30, 150.0))) == [4.0, 3.0, 3.0]

    def test_volume_leaves_the_trailing_window(self, renal: SofaRenalTransformer) -> None:
        # a good hour of output, then a charted but dry stretch: only instants
        # from 24h on are gradeable, and by 30h the volume has aged out
        urine = covered_urine((1, 600.0), *[(hours, 0.0) for hours in range(6, 31, 6)])
        assert renal_scores(renal, frame(), urine) == [0.0, 4.0]

    def test_stale_urine_stream_stops_scoring(self, renal: SofaRenalTransformer) -> None:
        # charting stops after 24h; by 60h the segment has ended and the criterion
        # goes quiet rather than reporting an empty window as anuria
        assert renal_scores(renal, frame((1, at(60), 0.5)), covered_urine((24, 600.0))) == [0.0, 0.0]

    def test_subjects_are_independent(self, renal: SofaRenalTransformer) -> None:
        crea = frame((1, T0, 5.5), (2, T0, 0.5))
        result = renal.transform({"creatinine": crea, "urine_output": frame()}).collect().sort("subject_id")
        assert result["numeric_value"].to_list() == [4.0, 0.0]


# --- the total ----------------------------------------------------------------


class TestTotal:
    def test_total_is_a_windowed_sum_of_components(self) -> None:
        total = make(SofaTransformer, terms=["sofa_renal", "sofa_cns"], window="24h")
        out = scores(total, {"sofa_renal": frame((1, T0, 2.0)), "sofa_cns": frame((1, at(1), 1.0))})
        # T0: renal 2 (cns not yet measured -> 0); T0+1h: renal 2 carried + cns 1
        assert out == [2.0, 3.0]

    def test_a_component_outside_the_window_contributes_zero(self) -> None:
        total = make(SofaTransformer, terms=["sofa_renal", "sofa_cns"], window="24h")
        out = scores(total, {"sofa_renal": frame((1, T0, 2.0)), "sofa_cns": frame((1, at(48), 1.0))})
        assert out == [2.0, 1.0]

    def test_terms_default_to_the_declared_dependencies(self) -> None:
        assert scores(make(SofaTransformer), {"sofa_liver": frame((1, T0, 3.0))}) == [3.0]


# --- end-to-end ---------------------------------------------------------------

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


def _concept_yml(name: str, unit: str, extensions: tuple[str, ...] = ("dataset",)) -> str:
    columns = "".join(f'  {extension}: col("{extension}")\n' for extension in extensions)
    return f"name: {name}\nversion: 1.0.0\nunit: {unit}\nextension_columns:\n{columns}"


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


def _complex_mapping_yml(transformer: str, inputs: list[str], *, as_terms: bool = False) -> str:
    versioned = "".join(f"  - {name}.1.0.0\n" for name in inputs)
    mapping = f"type: complex\nconcept_transformer: {transformer}\nconcepts:\n{versioned}"
    if as_terms:
        named = "".join(f"    - {name}\n" for name in inputs)
        mapping += f"kwargs:\n  terms:\n{named}"
    return mapping


def _run_pipeline(
    tmp_path: Path,
    labs_csv: str,
    concepts: list[tuple[str, str]] | list[tuple[str, str, tuple[str, ...]]],
    mappings: dict[str, str],
) -> OpenICUProject:
    """Write a minimal project around one labs table and run both steps."""
    data_dir = tmp_path / "data" / DATASET
    data_dir.mkdir(parents=True)
    (data_dir / "labs.csv").write_text(labs_csv)

    table_dir = tmp_path / "config" / DATASET / "1.0" / "tables"
    table_dir.mkdir(parents=True)
    (table_dir / "labs.yml").write_text(LABS_TABLE_YML)

    concept_dir = tmp_path / "config" / "concepts"
    concept_dir.mkdir(parents=True)
    for entry in concepts:
        name, unit, *rest = entry
        extensions = rest[0] if rest else ("dataset",)
        (concept_dir / f"{name}.yml").write_text(_concept_yml(name, unit, extensions))

    mapping_dir = tmp_path / "config" / DATASET / "1.0" / "mappings"
    mapping_dir.mkdir(parents=True)
    for name, mapping in mappings.items():
        (mapping_dir / f"{name}.yml").write_text(mapping)

    (tmp_path / "extraction.yml").write_text(
        "name: Extraction\nversion: 1.0.0\n\nconfig:\n  data:\n"
        f'    - name: {DATASET}\n      version: "1.0"\n      path: {data_dir}\n'
    )
    (tmp_path / "concept.yml").write_text(
        "name: Concept\nversion: 1.0.0\n\nconfig:\n"
        "  extraction_step: Extraction\n"
        f'  mapping_configs:\n    - name: {DATASET}\n      version: "1.0"\n'
    )

    project = OpenICUProject(tmp_path / "project")
    load_extracation_config(table_dir)
    load_concept_config(concept_dir, [mapping_dir])
    ExtractionStep.load(project, tmp_path / "extraction.yml").run()
    ConceptStep.load(project, tmp_path / "concept.yml").run()
    return project


def _concept_output(project: OpenICUProject, name: str) -> pl.DataFrame:
    path = project.datasets_path / "concept" / "data" / name / "1.0.0" / f"{DATASET}.parquet"
    return pl.read_parquet(path).sort("time")


RENAL_LABS_CSV = """\
subject_id,charttime,itemid,valuenum
1,2024-01-01 00:00:00,CREA,2.0
1,2024-01-01 00:00:00,URINE,300
1,2024-01-01 02:00:00,CREA,5.5
"""

CHAIN_LABS_CSV = """\
subject_id,charttime,itemid,valuenum
1,2024-01-01 00:00:00,EYE,1
1,2024-01-01 00:00:00,MOTOR,1
1,2024-01-01 00:00:00,VERBAL,1
1,2024-01-01 00:00:00,PLT,30
1,2024-01-01 01:00:00,PLT,200
"""


def test_end_to_end_sofa_renal_concept(tmp_path: Path) -> None:
    """Extraction -> simple crea/urine concepts -> the complex renal component."""
    renal_transformer = "open_icu.steps.concept.transformer.sofa.SofaRenalTransformer"
    project = _run_pipeline(
        tmp_path,
        RENAL_LABS_CSV,
        concepts=[
            ("creatinine", "mg/dL", ("dataset", "table")),
            ("urine_output", "mL", ("dataset", "table")),
            ("sofa_renal", "points", ("dataset",)),
        ],
        mappings={
            "creatinine": _simple_mapping_yml("CREA"),
            "urine_output": _simple_mapping_yml("URINE"),
            "sofa_renal": _complex_mapping_yml(renal_transformer, ["creatinine", "urine_output"]),
        },
    )

    df = _concept_output(project, "sofa_renal")

    # A single urine record covers no window, so the volume criterion never grades
    # and the score is creatinine-driven throughout.
    # 00:00 -> crea 2.0 -> 2
    # 02:00 -> crea 5.5 -> 4
    assert df["code"].to_list() == ["sofa_renal//points", "sofa_renal//points"]
    assert df["numeric_value"].to_list() == [2.0, 4.0]
    assert df.schema["numeric_value"] == pl.Float32
    assert df["dataset"].unique().to_list() == [DATASET]

    codes = pl.read_parquet(project.datasets_path / "concept" / "metadata" / "codes.parquet")
    assert "sofa_renal//points" in codes["code"].to_list()


def test_end_to_end_total_sofa_chain(tmp_path: Path) -> None:
    """The deepest dependency chain: complex concepts depending on complex ones.

    GCS parts -> GCS_total (sum) -> sofa_cns, platelets -> sofa_coagulation,
    and sofa (total) -> both components. Exercises ordering and cross-timestamp
    alignment through the real ConceptStep.
    """
    win_sum = "open_icu.steps.concept.transformer.windowed.WindowedSumTransformer"
    sofa_total = "open_icu.steps.concept.transformer.sofa.SofaTransformer"
    sofa_cns = "open_icu.steps.concept.transformer.sofa.SofaCnsTransformer"
    sofa_coag = "open_icu.steps.concept.transformer.sofa.SofaCoagulationTransformer"

    project = _run_pipeline(
        tmp_path,
        CHAIN_LABS_CSV,
        concepts=[
            ("GCS_eye", "points"),
            ("GCS_motor", "points"),
            ("GCS_verbal", "points"),
            ("GCS_total", "points"),
            ("platelet_count", "K/uL"),
            ("sofa_cns", "points"),
            ("sofa_coagulation", "points"),
            ("sofa", "points"),
        ],
        mappings={
            "GCS_eye": _simple_mapping_yml("EYE"),
            "GCS_motor": _simple_mapping_yml("MOTOR"),
            "GCS_verbal": _simple_mapping_yml("VERBAL"),
            "platelet_count": _simple_mapping_yml("PLT"),
            "GCS_total": _complex_mapping_yml(
                win_sum, ["GCS_eye", "GCS_motor", "GCS_verbal"], as_terms=True
            ),
            "sofa_cns": _complex_mapping_yml(sofa_cns, ["GCS_total"]),
            "sofa_coagulation": _complex_mapping_yml(sofa_coag, ["platelet_count"]),
            "sofa": _complex_mapping_yml(sofa_total, ["sofa_cns", "sofa_coagulation"], as_terms=True),
        },
    )

    sofa = _concept_output(project, "sofa")

    # GCS 1+1+1=3 -> CNS 4; PLT 30 -> coag 3; total 7 at 00:00.
    # At 01:00 PLT 200 -> coag 0, CNS 4 carried forward -> total 4.
    assert sofa["code"].to_list() == ["sofa//points", "sofa//points"]
    assert sofa["numeric_value"].to_list() == [7.0, 4.0]
