# ruff: noqa: E402

from pathlib import Path

import pytest
import yaml

pl = pytest.importorskip("polars")

from open_icu.adapter.converters.ricu.validation.compare_outputs import (
    compare_output_dirs,
    extract_dependencies_from_dataset_concept_config,
    load_concept_config_index,
    summarize_results,
)


def test_extract_derived_dependencies():
    data = {
        "type": "derived",
        "table": {"concept": "patient_weight"},
        "join": [
            {"concept": "patient_height"},
            {"concept": "concept/fraction_of_inspired_oxygen"},
        ],
    }

    assert extract_dependencies_from_dataset_concept_config(data) == [
        "patient_weight",
        "patient_height",
        "fraction_of_inspired_oxygen",
    ]


def test_extract_complex_dependencies():
    data = {"type": "complex", "concepts": ["respiratory_rate", "concept/oxygen_saturation"]}
    assert extract_dependencies_from_dataset_concept_config(data) == [
        "respiratory_rate",
        "oxygen_saturation",
    ]


def test_compare_outputs_with_derived_dependency_check(tmp_path: Path):
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    config_root = tmp_path / "config"

    for root in [old_root, new_root]:
        out = root / "body_mass_index" / "1.0.0"
        out.mkdir(parents=True)
        pl.DataFrame(
            {
                "subject_id": [1],
                "time": ["2020-01-01"],
                "code": ["body_mass_index"],
                "numeric_value": [22.0],
            }
        ).write_parquet(out / "mimic-iv.parquet")

    # Only one dependency exists in new output; patient_height is missing.
    dep = new_root / "patient_weight" / "1.0.0"
    dep.mkdir(parents=True)
    pl.DataFrame({"subject_id": [1], "time": ["2020-01-01"], "numeric_value": [80.0]}).write_parquet(
        dep / "mimic-iv.parquet"
    )

    cfg = config_root / "dataset" / "mimic-iv" / "3.1" / "concept"
    cfg.mkdir(parents=True)
    (cfg / "body_mass_index.yml").write_text(
        yaml.safe_dump(
            {
                "type": "derived",
                "table": {"concept": "patient_weight"},
                "join": [{"concept": "patient_height"}],
            }
        ),
        encoding="utf-8",
    )

    results = compare_output_dirs(
        old_root=old_root,
        new_root=new_root,
        config_root=config_root,
        key_columns=["subject_id", "time", "code"],
    )

    bmi = next(result for result in results if result.concept == "body_mass_index")
    assert bmi.concept_type == "derived"
    assert bmi.status == "dependency_missing"
    assert [check.concept for check in bmi.dependency_checks] == [
        "patient_weight",
        "patient_height",
    ]
    assert [check.exists_in_new for check in bmi.dependency_checks] == [True, False]

    summary = summarize_results(results)
    assert summary["concept_type_counts"]["derived"] == 1


def test_load_concept_config_index_merges_dependencies(tmp_path: Path):
    cfg = tmp_path / "dataset" / "mimic-iv" / "3.1" / "concept"
    cfg.mkdir(parents=True)
    (cfg / "x.yml").write_text(
        yaml.safe_dump({"type": "complex", "concepts": ["a", "b", "a"]}),
        encoding="utf-8",
    )
    index = load_concept_config_index(tmp_path)
    assert index["x"]["concept_type"] == "complex"
    assert index["x"]["dependencies"] == ["a", "b"]


def test_compare_outputs_ignore_columns(tmp_path: Path):
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"

    old_out = old_root / "x" / "1.0.0"
    new_out = new_root / "x" / "1.0.0"
    old_out.mkdir(parents=True)
    new_out.mkdir(parents=True)

    pl.DataFrame(
        {
            "subject_id": [1],
            "time": ["2020-01-01"],
            "code": ["x"],
            "numeric_value": [1.0],
        }
    ).write_parquet(old_out / "mimic-iv.parquet")
    pl.DataFrame(
        {
            "subject_id": [1],
            "time": ["2020-01-01"],
            "code": ["x"],
            "numeric_value": [1.0],
            "dataset": ["mimic-iv"],
            "table": ["derived"],
        }
    ).write_parquet(new_out / "mimic-iv.parquet")

    strict = compare_output_dirs(old_root=old_root, new_root=new_root)
    assert strict[0].status == "schema_changed"

    ignored = compare_output_dirs(
        old_root=old_root,
        new_root=new_root,
        ignore_columns=["dataset", "table"],
    )
    assert ignored[0].status == "unchanged"


def test_compare_outputs_normalize_code_hyphen_to_underscore(tmp_path: Path):
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"

    old_out = old_root / "C_reactive_protein" / "1.0.0"
    new_out = new_root / "C_reactive_protein" / "1.0.0"
    old_out.mkdir(parents=True)
    new_out.mkdir(parents=True)

    common = {
        "subject_id": [1],
        "time": ["2020-01-01"],
        "numeric_value": [7.0],
        "text_value": [None],
    }
    pl.DataFrame({**common, "code": ["C-reactive_protein//mg/L"]}).write_parquet(
        old_out / "mimic-iv.parquet"
    )
    pl.DataFrame({**common, "code": ["C_reactive_protein//mg/L"]}).write_parquet(
        new_out / "mimic-iv.parquet"
    )

    strict = compare_output_dirs(
        old_root=old_root,
        new_root=new_root,
        key_columns=["subject_id", "time", "code"],
    )
    assert strict[0].status == "changed"
    assert strict[0].missing_in_new == 1
    assert strict[0].missing_in_old == 1

    normalized = compare_output_dirs(
        old_root=old_root,
        new_root=new_root,
        key_columns=["subject_id", "time", "code"],
        normalize_code="hyphen-to-underscore",
    )
    assert normalized[0].status == "unchanged"
