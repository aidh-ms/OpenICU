"""Shared fixtures for step integration tests.

Builds a tiny synthetic ICU dataset ("testdb") with a vitals table and an item
lookup table, plus the matching extraction and concept configurations, so the
steps can be exercised end-to-end without any real data.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest

from open_icu.steps.concept.registry import concept_config_registry
from open_icu.steps.extraction.registry import dataset_config_registry

VITALS_CSV = """\
subject_id,stay_id,charttime,itemid,valuenum,valueuom,value
1,100,2024-01-01 08:00:00,220045,80,bpm,eighty
1,100,2024-01-01 09:00:00,220045,82,bpm,
2,200,2024-01-02 10:00:00,220050,120,mmHg,
2,200,2024-01-02 11:00:00,999999,5,units,
"""

ITEMS_CSV = """\
itemid,label
220045,Heart Rate
220050,Systolic BP
"""

MEASUREMENTS_CSV = """\
subject_id,stay_id,charttime,weight_kg,height_m
1,100,2024-01-01 08:00:00,80,2.0
2,200,2024-01-02 10:00:00,60,1.5
"""

VITALS_TABLE_YML = """\
path: vitals.csv
columns:
  - name: subject_id
    type: int64
  - name: stay_id
    type: int64
  - name: charttime
    type: datetime
    params:
      format: "%Y-%m-%d %H:%M:%S"
  - name: itemid
    type: int64
  - name: valuenum
    type: float32
  - name: valueuom
    type: string
  - name: value
    type: string

join:
  - path: items.csv
    columns:
      - name: itemid
        type: int64
      - name: label
        type: string
    both_on:
      - itemid

event_defaults:
  subject_id: col(subject_id)
  extension:
    stay_id: col(stay_id)

events:
  - name: CHART
    columns:
      time: col(charttime)
      code:
        - col(itemid)
        - col(label)
        - col(valueuom)
      numeric_value: col(valuenum)
      text_value: col(value)
"""

MEASUREMENTS_TABLE_YML = """\
path: measurements.csv
columns:
  - name: subject_id
    type: int64
  - name: charttime
    type: datetime
    params:
      format: "%Y-%m-%d %H:%M:%S"
  - name: weight_kg
    type: float32
  - name: height_m
    type: float32

event_defaults:
  subject_id: col(subject_id)
  time: col(charttime)

events:
  - name: WEIGHT
    columns:
      code:
        - const(kg)
      numeric_value: col(weight_kg)
  - name: HEIGHT
    columns:
      code:
        - const(m)
      numeric_value: col(height_m)
"""


@pytest.fixture(autouse=True)
def clean_registries() -> Iterator[None]:
    """Isolate tests from the global configuration registry singletons."""
    dataset_config_registry.clear()
    concept_config_registry.clear()
    yield
    dataset_config_registry.clear()
    concept_config_registry.clear()


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data" / "testdb"
    data_dir.mkdir(parents=True)
    (data_dir / "vitals.csv").write_text(VITALS_CSV)
    (data_dir / "items.csv").write_text(ITEMS_CSV)
    (data_dir / "measurements.csv").write_text(MEASUREMENTS_CSV)
    return data_dir


@pytest.fixture
def table_config_dir(tmp_path: Path) -> Path:
    config_dir = tmp_path / "config" / "testdb" / "1.0" / "tables"
    config_dir.mkdir(parents=True)
    (config_dir / "vitals.yml").write_text(VITALS_TABLE_YML)
    (config_dir / "measurements.yml").write_text(MEASUREMENTS_TABLE_YML)
    return config_dir


@pytest.fixture
def extraction_config(tmp_path: Path, data_dir: Path, table_config_dir: Path) -> Path:
    config_file = tmp_path / "extraction.yml"
    config_file.write_text(
        f"""\
name: Extraction
version: 1.0.0

config_files:
  - path: {table_config_dir}

config:
  data:
    - name: testdb
      path: {data_dir}
"""
    )
    return config_file


@pytest.fixture
def concept_config(tmp_path: Path) -> Path:
    """Concept dictionary + per-dataset mappings + concept step config."""
    concept_dir = tmp_path / "config" / "concepts"
    concept_dir.mkdir(parents=True)
    (concept_dir / "heart_rate.yml").write_text(
        """\
name: heart_rate
version: 1.0.0
unit: bpm
extension_columns:
  dataset: col("dataset")
  table: col("table")
"""
    )
    (concept_dir / "patient_weight.yml").write_text("name: patient_weight\nversion: 1.0.0\nunit: kg\n")
    (concept_dir / "patient_height.yml").write_text("name: patient_height\nversion: 1.0.0\nunit: m\n")
    (concept_dir / "bmi.yml").write_text("name: bmi\nversion: 1.0.0\nunit: kg/m2\n")

    mapping_dir = tmp_path / "config" / "testdb" / "1.0" / "mappings"
    mapping_dir.mkdir(parents=True)
    (mapping_dir / "heart_rate.yml").write_text(
        """\
type: simple
mappings:
  - pattern:
      table: vitals
      event: CHART
      code: (220045//Heart Rate)
    columns:
      numeric_value: col(numeric_value)
      text_value: col(text_value)
"""
    )
    (mapping_dir / "patient_weight.yml").write_text(
        """\
type: simple
mappings:
  - pattern:
      table: measurements
      event: WEIGHT
      code: kg
    columns:
      numeric_value: col(numeric_value)
"""
    )
    (mapping_dir / "patient_height.yml").write_text(
        """\
type: simple
mappings:
  - pattern:
      table: measurements
      event: HEIGHT
      code: m
    columns:
      numeric_value: col(numeric_value)
"""
    )
    (mapping_dir / "bmi.yml").write_text(
        """\
type: derived
table:
  concept: patient_weight.1.0.0
  columns:
    - subject_id
    - time
    - numeric_value
    - text_value
join:
  - concept: patient_height.1.0.0
    columns:
      - subject_id
      - time
      - numeric_value
    both_on:
      - subject_id
      - time
    how: inner
event:
  numeric_value: divide(numeric_value, multiply(numeric_value_right, numeric_value_right))
"""
    )

    config_file = tmp_path / "concept.yml"
    config_file.write_text(
        f"""\
name: Concept
version: 1.0.0

config_files:
  - path: {tmp_path / "config" / "concepts"}

config:
  extraction_step: Extraction
  dataset_configs:
    - name: testdb
      path: {mapping_dir}
"""
    )
    return config_file
