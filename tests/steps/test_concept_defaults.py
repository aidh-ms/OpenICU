"""A concept-level `default` applies only where the dataset supplies every input.

The mechanism itself is covered in ``test_concept_config.py``; these run it
through the real ConceptStep, because the rule is about what a dataset could
produce and that is only known once its dependencies have had their turn.
"""

from pathlib import Path

import polars as pl

from open_icu import ConceptStep, ExtractionStep, OpenICUProject
from tests.steps.conftest import load_concept_config, load_extracation_config

DATASET = "testdb"
SUM_TRANSFORMER = "open_icu.steps.concept.transformer.windowed.WindowedSumTransformer"

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

# only `a` is ever charted, so a concept needing `b` cannot be satisfied here
LABS_CSV = """\
subject_id,charttime,itemid,valuenum
1,2024-01-01 00:00:00,A,3
1,2024-01-01 01:00:00,A,4
"""

SIMPLE_A = (
    "type: simple\n"
    "mappings:\n"
    "  - pattern:\n"
    "      table: labs\n"
    "      event: LAB\n"
    "      code: A\n"
    "    columns:\n"
    "      numeric_value: col(numeric_value)\n"
)

# the same definition, written once and rendered either as a dataset mapping
# (top level) or as a concept-level default (indented under `default:`)
TOTAL_DEFINITION = (
    "type: complex\n"
    f"concept_transformer: {SUM_TRANSFORMER}\n"
    "concepts:\n"
    "  - a.1.0.0\n"
    "  - b.1.0.0\n"
    "kwargs:\n"
    "  terms:\n"
    "    - a\n"
    "    - b\n"
)
TOTAL_AS_DEFAULT = "".join(f"  {line}\n" for line in TOTAL_DEFINITION.splitlines())


def _concept_yml(name: str, default: str | None = None) -> str:
    body = f'name: {name}\nversion: 1.0.0\nunit: points\nextension_columns:\n  dataset: col("dataset")\n'
    if default is not None:
        body += f"default:\n{default}"
    return body


def _run(tmp_path: Path, *, total_mapping: str | None) -> OpenICUProject:
    """Run both steps over a dataset that can supply `a` but never `b`."""
    data_dir = tmp_path / "data" / DATASET
    data_dir.mkdir(parents=True)
    (data_dir / "labs.csv").write_text(LABS_CSV)

    table_dir = tmp_path / "config" / DATASET / "1.0" / "tables"
    table_dir.mkdir(parents=True)
    (table_dir / "labs.yml").write_text(LABS_TABLE_YML)

    concept_dir = tmp_path / "config" / "concepts"
    concept_dir.mkdir(parents=True)
    (concept_dir / "a.yml").write_text(_concept_yml("a"))
    (concept_dir / "b.yml").write_text(_concept_yml("b"))
    (concept_dir / "total.yml").write_text(_concept_yml("total", TOTAL_AS_DEFAULT))

    mapping_dir = tmp_path / "config" / DATASET / "1.0" / "mappings"
    mapping_dir.mkdir(parents=True)
    (mapping_dir / "a.yml").write_text(SIMPLE_A)
    # no mapping for `b`: the dataset simply cannot produce it
    if total_mapping is not None:
        (mapping_dir / "total.yml").write_text(total_mapping)

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


def _output(project: OpenICUProject, name: str) -> Path:
    return project.datasets_path / "concept" / "data" / name / "1.0.0" / f"{DATASET}.parquet"


def test_default_is_withheld_when_an_input_cannot_be_produced(tmp_path: Path) -> None:
    project = _run(tmp_path, total_mapping=None)

    # `a` resolves, so the dataset is not simply empty
    assert _output(project, "a").exists()
    # ...but `b` never can, so the total is not published on the dataset's behalf
    assert not _output(project, "total").exists()


def test_an_explicit_mapping_still_computes_from_what_is_available(tmp_path: Path) -> None:
    """Writing the mapping is the opt-in, and keeps the laxer behaviour."""
    project = _run(tmp_path, total_mapping=TOTAL_DEFINITION)

    total = _output(project, "total")
    assert total.exists()
    # the missing term contributes nothing rather than blocking the score
    assert pl.read_parquet(total)["numeric_value"].to_list() == [3.0, 4.0]
