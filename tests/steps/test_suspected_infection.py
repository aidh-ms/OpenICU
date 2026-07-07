"""Tests for the Sepsis-3 suspected-infection transformer.

The concept emits one event per suspected-infection onset, valued 1, dated at
``min(antibiotic, culture)`` — the *earlier* of the qualifying pair (Seymour).
Detection is at the later event but the emit time is re-dated to the onset.
Inputs are marker events with a *null* numeric_value (antibiotics/cultures are
boolean concepts), so this also exercises event-presence detection.
"""

from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from open_icu import ConceptStep, ExtractionStep, OpenICUProject
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.transformers import SuspectedInfectionTransformer
from tests.steps.conftest import load_concept_config, load_extracation_config

T0 = datetime(2024, 1, 1, 0, 0)


@pytest.fixture
def transformer() -> SuspectedInfectionTransformer:
    concept = ConceptConfig(name="suspected_infection", version="1.0.0", unit="boolean")
    config = ComplexDatasetConceptConfig(
        name="suspected_infection", version="1.0", dataset="testdb", concept_transformer="unused"
    )
    return SuspectedInfectionTransformer(concept, config)


def markers(*hours: float) -> pl.LazyFrame:
    """Marker events (null numeric_value) at the given hour offsets from T0."""
    times = [T0 + timedelta(hours=h) for h in hours]
    return pl.LazyFrame(
        {"subject_id": [1] * len(times), "time": times, "numeric_value": [None] * len(times)},
        schema={"subject_id": pl.Int64, "time": pl.Datetime(time_unit="us"), "numeric_value": pl.Float32},
    )


def onsets(transformer: SuspectedInfectionTransformer, abx: list[float], samp: list[float]) -> list:
    """Return [(onset_hour, value), ...] for the given antibiotic/culture hours."""
    out = (
        transformer.transform({"antibiotics": markers(*abx), "body_fluid_sampling": markers(*samp)})
        .collect()
        .sort("time")
    )
    return [((t - T0).total_seconds() / 3600.0, v) for t, v in zip(out["time"], out["numeric_value"])]


def test_antibiotic_first_onset_is_the_antibiotic(transformer: SuspectedInfectionTransformer) -> None:
    # abx at 0h, culture at +10h: detected at the culture, dated at the antibiotic
    assert onsets(transformer, abx=[0], samp=[10]) == [(0.0, 1.0)]


def test_culture_first_onset_is_the_culture(transformer: SuspectedInfectionTransformer) -> None:
    # culture at 0h, abx at +50h: detected at the antibiotic, dated at the culture
    assert onsets(transformer, abx=[50], samp=[0]) == [(0.0, 1.0)]


def test_antibiotic_first_culture_too_late(transformer: SuspectedInfectionTransformer) -> None:
    assert onsets(transformer, abx=[0], samp=[30]) == []


def test_culture_first_antibiotic_too_late(transformer: SuspectedInfectionTransformer) -> None:
    assert onsets(transformer, abx=[80], samp=[0]) == []


def test_simultaneous_events(transformer: SuspectedInfectionTransformer) -> None:
    assert onsets(transformer, abx=[0], samp=[0]) == [(0.0, 1.0)]


def test_antibiotic_alone_is_not_infection(transformer: SuspectedInfectionTransformer) -> None:
    assert onsets(transformer, abx=[0], samp=[]) == []


def test_culture_alone_is_not_infection(transformer: SuspectedInfectionTransformer) -> None:
    assert onsets(transformer, abx=[], samp=[0]) == []


def test_window_boundaries(transformer: SuspectedInfectionTransformer) -> None:
    # culture exactly 24h after abx: within the antibiotic-first window, onset at abx
    assert onsets(transformer, abx=[0], samp=[24]) == [(0.0, 1.0)]
    # antibiotic exactly 72h after culture: within the culture-first window, onset at culture
    assert onsets(transformer, abx=[72], samp=[0]) == [(0.0, 1.0)]


def test_repeated_cultures_collapse_to_one_onset(transformer: SuspectedInfectionTransformer) -> None:
    # one antibiotic, two later cultures both within 24h -> a single onset at the antibiotic
    assert onsets(transformer, abx=[0], samp=[10, 20]) == [(0.0, 1.0)]


def test_distinct_episodes_are_kept(transformer: SuspectedInfectionTransformer) -> None:
    # two separate antibiotic+culture pairs a week apart -> two onsets
    assert onsets(transformer, abx=[0, 200], samp=[10, 210]) == [(0.0, 1.0), (200.0, 1.0)]


# --- end-to-end: marker events (null numeric_value) through the real pipeline ---
LABS_CSV = """\
subject_id,charttime,itemid,valuenum
1,2024-01-01 00:00:00,ABX,
1,2024-01-01 10:00:00,CULT,
1,2024-01-05 00:00:00,CULT,
"""

LABS_TABLE_YML = """\
path: labs.csv
columns:
  - {name: subject_id, type: int64}
  - {name: charttime, type: datetime, params: {format: "%Y-%m-%d %H:%M:%S"}}
  - {name: itemid, type: string}
  - {name: valuenum, type: float32}
event_defaults:
  subject_id: col(subject_id)
  time: col(charttime)
events:
  - name: LAB
    columns:
      code: [col(itemid)]
      numeric_value: col(valuenum)
"""


def _simple(code: str) -> str:
    return (
        f"type: simple\nmappings:\n  - pattern:\n      table: labs\n      event: LAB\n"
        f"      code: {code}\n    columns:\n      numeric_value: col(numeric_value)\n"
    )


def test_end_to_end_onset_dating(tmp_path: Path) -> None:
    data_dir = tmp_path / "data" / "testdb"
    data_dir.mkdir(parents=True)
    (data_dir / "labs.csv").write_text(LABS_CSV)

    table_dir = tmp_path / "cfg" / "testdb" / "1.0" / "tables"
    table_dir.mkdir(parents=True)
    (table_dir / "labs.yml").write_text(LABS_TABLE_YML)

    concept_dir = tmp_path / "cfg" / "concepts"
    concept_dir.mkdir(parents=True)
    for name in ("antibiotics", "body_fluid_sampling", "suspected_infection"):
        (concept_dir / f"{name}.yml").write_text(
            f'name: {name}\nversion: 1.0.0\nunit: boolean\nextension_columns:\n  dataset: col("dataset")\n'
        )

    mapping_dir = tmp_path / "cfg" / "testdb" / "1.0" / "mappings"
    mapping_dir.mkdir(parents=True)
    (mapping_dir / "antibiotics.yml").write_text(_simple("ABX"))
    (mapping_dir / "body_fluid_sampling.yml").write_text(_simple("CULT"))
    (mapping_dir / "suspected_infection.yml").write_text(
        "type: complex\n"
        "concept_transformer: open_icu.steps.concept.transformers.sepsis.SuspectedInfectionTransformer\n"
        "concepts:\n  - antibiotics.1.0.0\n  - body_fluid_sampling.1.0.0\n"
    )

    (tmp_path / "extraction.yml").write_text(
        f'name: Extraction\nversion: 1.0.0\nconfig:\n  data:\n    - name: testdb\n      version: "1.0"\n      path: {data_dir}\n'
    )
    (tmp_path / "concept.yml").write_text(
        "name: Concept\nversion: 1.0.0\nconfig:\n  extraction_step: Extraction\n"
        '  mapping_configs:\n    - name: testdb\n      version: "1.0"\n'
    )

    project = OpenICUProject(tmp_path / "project")
    load_extracation_config(table_dir)
    load_concept_config(concept_dir, [mapping_dir])
    ExtractionStep.load(project, tmp_path / "extraction.yml").run()
    ConceptStep.load(project, tmp_path / "concept.yml").run()

    df = pl.read_parquet(
        project.datasets_path / "concept" / "data" / "suspected_infection" / "1.0.0" / "testdb.parquet"
    ).sort("time")

    # antibiotic 00:00 + culture 10:00 (<=24h) -> one onset dated at the antibiotic;
    # the day-4 culture has no antibiotic within 24h -> nothing.
    assert df["numeric_value"].to_list() == [1.0]
    assert df["time"].to_list() == [datetime(2024, 1, 1, 0, 0)]
    assert df["code"].to_list() == ["suspected_infection//boolean"]
