"""Tests for the sharding step."""

from pathlib import Path

import polars as pl

from open_icu.steps.sharding.config.step import ShardingStepConfig
from open_icu.steps.sharding.step import ShardingStep
from open_icu.storage.project import OpenICUProject


def write_concept_file(path: Path, subject_ids: list[int], code: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        {
            "subject_id": subject_ids,
            "time": [None] * len(subject_ids),
            "code": [code] * len(subject_ids),
            "numeric_value": [float(i) for i in subject_ids],
            "text_value": [None] * len(subject_ids),
            # Extra columns are intentionally different between concept files.
            # The sharding step keeps the stable MEDS-like long columns only.
            "source_column": ["source"] * len(subject_ids),
        }
    ).with_columns(pl.col("time").cast(pl.Datetime("us")))
    df.write_parquet(path)


def test_sharding_config_defaults(tmp_path: Path) -> None:
    config_file = tmp_path / "sharding.yml"
    config_file.write_text(
        """\
name: Sharding
version: 1.0.0

config:
  concept_step: Concept
"""
    )

    config = ShardingStepConfig.load(config_file)

    assert config.name == "Sharding"
    assert config.config.concept_step == "Concept"
    assert config.config.datasets == []
    assert config.config.concepts == []
    assert config.config.subjects == []
    assert config.config.subjects_per_shard == 1000


def test_sharding_writes_subject_grouped_long_format_shards(tmp_path: Path) -> None:
    project_path = tmp_path / "project"
    config_file = tmp_path / "sharding.yml"
    config_file.write_text(
        """\
name: Sharding
version: 1.0.0
overwrite: true

config:
  concept_step: Concept
  datasets:
    - testdb
  concepts:
    - heart_rate
  subjects_per_shard: 2
"""
    )

    with OpenICUProject(project_path) as project:
        concept_dataset = project.add_dataset("concept")
        write_concept_file(
            concept_dataset.data_path / "heart_rate" / "1.0.0" / "testdb.parquet",
            [1, 2, 3],
            "heart_rate//bpm",
        )
        write_concept_file(
            concept_dataset.data_path / "lactate" / "1.0.0" / "otherdb.parquet",
            [1, 2, 3],
            "lactate//mmol/l",
        )

        ShardingStep.load(project, config_file).run()

    output_files = sorted((project_path / "datasets" / "sharding" / "data").glob("*.parquet"))
    assert [file.name for file in output_files] == ["shard_00000.parquet", "shard_00001.parquet"]

    shard_0 = pl.read_parquet(output_files[0])
    shard_1 = pl.read_parquet(output_files[1])

    assert shard_0.columns == ["subject_id", "time", "code", "numeric_value", "text_value"]
    assert shard_0["subject_id"].to_list() == [1, 2]
    assert shard_1["subject_id"].to_list() == [3]
    assert shard_0["code"].unique().to_list() == ["heart_rate//bpm"]


def test_sharding_filters_configured_subjects(tmp_path: Path) -> None:
    project_path = tmp_path / "project"
    config_file = tmp_path / "sharding.yml"
    config_file.write_text(
        """\
name: Sharding
version: 1.0.0
overwrite: true

config:
  concept_step: Concept
  subjects:
    - 2
  subjects_per_shard: 100
"""
    )

    with OpenICUProject(project_path) as project:
        concept_dataset = project.add_dataset("concept")
        write_concept_file(
            concept_dataset.data_path / "heart_rate" / "1.0.0" / "testdb.parquet",
            [1, 2, 3],
            "heart_rate//bpm",
        )

        ShardingStep.load(project, config_file).run()

    output_file = project_path / "datasets" / "sharding" / "data" / "shard_00000.parquet"
    shard = pl.read_parquet(output_file)

    assert shard["subject_id"].to_list() == [2]


def test_sharding_filters_concepts_using_real_concept_output_structure(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project"
    config_file = tmp_path / "sharding.yml"
    config_file.write_text(
        """\
name: Sharding
version: 1.0.0
overwrite: true

config:
  concept_step: Concept
  concepts:
    - heart_rate
  subjects_per_shard: 100
"""
    )

    with OpenICUProject(project_path) as project:
        concept_dataset = project.add_dataset("concept")

        write_concept_file(
            concept_dataset.data_path / "heart_rate" / "1.0.0" / "testdb.parquet",
            [1, 2],
            "heart_rate//bpm",
        )
        write_concept_file(
            concept_dataset.data_path / "lactate" / "1.0.0" / "testdb.parquet",
            [1, 2],
            "lactate//mmol/l",
        )

        ShardingStep.load(project, config_file).run()

    output_file = project_path / "datasets" / "sharding" / "data" / "shard_00000.parquet"
    shard = pl.read_parquet(output_file)

    assert shard["code"].unique().to_list() == ["heart_rate//bpm"]


def test_sharding_orders_same_time_events_semantically(tmp_path: Path) -> None:
    project_path = tmp_path / "project"
    config_file = tmp_path / "sharding.yml"
    config_file.write_text(
        """\
name: Sharding
version: 1.0.0
overwrite: true

config:
  concept_step: Concept
  subjects_per_shard: 100
"""
    )

    with OpenICUProject(project_path) as project:
        concept_dataset = project.add_dataset("concept")

        for concept, code in [
            ("sofa", "sofa//points"),
            ("gcs_total", "gcs_total//points"),
            ("sofa_cns", "sofa_cns//points"),
            ("gcs_eye", "gcs_eye//points"),
        ]:
            write_concept_file(
                concept_dataset.data_path / concept / "1.0.0" / "testdb.parquet",
                [1],
                code,
            )

        ShardingStep.load(project, config_file).run()

    output_file = project_path / "datasets" / "sharding" / "data" / "shard_00000.parquet"
    shard = pl.read_parquet(output_file)

    assert shard["code"].to_list() == [
        "gcs_eye//points",
        "gcs_total//points",
        "sofa_cns//points",
        "sofa//points",
    ]


def test_sharding_uses_custom_event_order_config(tmp_path: Path) -> None:
    project_path = tmp_path / "project"
    config_file = tmp_path / "sharding.yml"
    event_order_file = tmp_path / "event_order.yml"

    event_order_file.write_text(
        """\
default_order: 50
unassigned: ignore

groups:
  first:
    order: 10
    concepts:
      - sofa
  second:
    order: 20
    concepts:
      - heart_rate
"""
    )

    config_file.write_text(
        """\
name: Sharding
version: 1.0.0
overwrite: true

config:
  concept_step: Concept
  subjects_per_shard: 100
  event_order_config: event_order.yml
"""
    )

    with OpenICUProject(project_path) as project:
        concept_dataset = project.add_dataset("concept")

        write_concept_file(
            concept_dataset.data_path / "heart_rate" / "1.0.0" / "testdb.parquet",
            [1],
            "heart_rate//bpm",
        )
        write_concept_file(
            concept_dataset.data_path / "sofa" / "1.0.0" / "testdb.parquet",
            [1],
            "sofa//points",
        )

        ShardingStep.load(project, config_file).run()

    output_file = project_path / "datasets" / "sharding" / "data" / "shard_00000.parquet"
    shard = pl.read_parquet(output_file)

    assert shard["code"].to_list() == [
        "sofa//points",
        "heart_rate//bpm",
    ]


def test_sharding_errors_on_unassigned_event_order(tmp_path: Path) -> None:
    project_path = tmp_path / "project"
    config_file = tmp_path / "sharding.yml"
    event_order_file = tmp_path / "event_order.yml"

    event_order_file.write_text(
        """\
default_order: 50
unassigned: error

groups:
  configured:
    order: 10
    concepts:
      - sofa
"""
    )

    config_file.write_text(
        """\
name: Sharding
version: 1.0.0
overwrite: true

config:
  concept_step: Concept
  event_order_config: event_order.yml
"""
    )

    with OpenICUProject(project_path) as project:
        concept_dataset = project.add_dataset("concept")

        write_concept_file(
            concept_dataset.data_path / "heart_rate" / "1.0.0" / "testdb.parquet",
            [1],
            "heart_rate//bpm",
        )

        try:
            ShardingStep.load(project, config_file).run()
        except ValueError as exc:
            assert "heart_rate" in str(exc)
        else:
            raise AssertionError("Expected ValueError for unassigned concept")


def test_event_order_rejects_duplicate_concept_assignment(tmp_path: Path) -> None:
    from pydantic import ValidationError

    from open_icu.config.event_order import EventOrderConfig

    event_order_file = tmp_path / "event_order.yml"
    event_order_file.write_text(
        """\
default_order: 50
unassigned: warn

groups:
  first:
    order: 10
    concepts:
      - sofa
  second:
    order: 20
    concepts:
      - sofa
"""
    )

    try:
        EventOrderConfig.load(event_order_file)
    except ValidationError as exc:
        assert "multiple event-order groups" in str(exc)
    else:
        raise AssertionError("Expected ValidationError for duplicate concept assignment")
