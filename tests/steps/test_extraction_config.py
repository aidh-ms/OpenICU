"""Tests for extraction step configuration models (table, event, column)."""

import polars as pl
import pytest

from open_icu.steps.extraction.config.table import JoinTableConfig, TableConfig, TableType


def make_table_config(**overrides) -> TableConfig:
    data = {
        "name": "labevents",
        "version": "3.1",
        "dataset": "mimic-iv",
        "path": "hosp/labevents.csv.gz",
        "columns": [
            {"name": "subject_id", "type": "int64"},
            {"name": "charttime", "type": "datetime", "params": {"format": "%Y-%m-%d %H:%M:%S"}},
            {"name": "valuenum", "type": "float32"},
        ],
        **overrides,
    }
    return TableConfig(**data)


class TestTableConfig:
    def test_dtypes_computed_from_columns(self) -> None:
        table = make_table_config()
        assert table.dtypes == {
            "subject_id": pl.Int64,
            "charttime": pl.String,  # datetimes are read as strings and parsed later
            "valuenum": pl.Float32,
        }

    def test_identifier_includes_dataset(self) -> None:
        table = make_table_config()
        assert table.identifier_tuple == ("table", "mimic-iv", "3.1", "labevents")
        assert table.identifier == "openicu.config.table.mimic-iv.3.1.labevents"


class TestTableTypeInference:
    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("measurement.parquet", TableType.PARQUET),
            ("data.pq", TableType.PARQUET),
            ("hosp/labevents.csv.gz", TableType.CSVGZ),
            ("vitals.csv", TableType.CSV),
            ("export_without_extension", TableType.PARQUET),  # default
        ],
    )
    def test_type_inferred_from_path(self, path: str, expected: TableType) -> None:
        assert make_table_config(path=path).type == expected

    def test_explicit_type_is_not_overridden(self) -> None:
        assert make_table_config(path="data.parquet", type="csvgz").type == TableType.CSVGZ

    def test_join_table_type_inferred_from_path(self) -> None:
        assert JoinTableConfig(path="d_items.csv.gz").type == TableType.CSVGZ
        assert JoinTableConfig(path="d_items.parquet").type == TableType.PARQUET


class TestEventDefaults:
    def test_defaults_fill_missing_event_columns(self) -> None:
        table = make_table_config(
            event_defaults={
                "subject_id": "col(subject_id)",
                "time": "col(charttime)",
                "extension": {"hadm_id": "col(hadm_id)"},
            },
            events=[{"name": "LAB", "columns": {"numeric_value": "col(valuenum)"}}],
        )

        event = table.events[0]
        assert event.columns.subject_id == "col(subject_id)"
        assert event.columns.time == "col(charttime)"
        assert event.columns.numeric_value == "col(valuenum)"
        assert event.columns.extension == {"hadm_id": "col(hadm_id)"}

    def test_event_values_override_defaults(self) -> None:
        table = make_table_config(
            event_defaults={"subject_id": "col(subject_id)", "time": "col(charttime)"},
            events=[
                {
                    "name": "LAB",
                    "columns": {"subject_id": "col(other_id)", "time": "col(storetime)"},
                }
            ],
        )
        assert table.events[0].columns.subject_id == "col(other_id)"
        assert table.events[0].columns.time == "col(storetime)"

    def test_explicit_empty_list_overrides_default_code(self) -> None:
        table = make_table_config(
            event_defaults={
                "subject_id": "col(subject_id)",
                "time": "col(charttime)",
                "code": ["col(label)"],
            },
            events=[
                {"name": "WITH_DEFAULT", "columns": {}},
                {"name": "EMPTIED", "columns": {"code": []}},
            ],
        )
        assert table.events[0].columns.code == ["col(label)"]
        assert table.events[1].columns.code == []

    def test_extension_defaults_are_merged_not_replaced(self) -> None:
        table = make_table_config(
            event_defaults={
                "subject_id": "col(subject_id)",
                "time": "col(charttime)",
                "extension": {"hadm_id": "col(hadm_id)"},
            },
            events=[
                {
                    "name": "LAB",
                    "columns": {"extension": {"stay_id": "col(stay_id)"}},
                }
            ],
        )
        assert table.events[0].columns.extension == {
            "hadm_id": "col(hadm_id)",
            "stay_id": "col(stay_id)",
        }

    def test_code_prefix_and_suffix_defaults(self) -> None:
        table = make_table_config(
            event_defaults={
                "subject_id": "col(subject_id)",
                "time": "col(charttime)",
                "code_prefix": ["const(PRE)"],
                "code_suffix": ["const(POST)"],
            },
            events=[
                {"name": "DEFAULTED", "columns": {}},
                {"name": "OVERRIDDEN", "code_prefix": [], "columns": {}},
            ],
        )
        assert table.events[0].code_prefix == ["const(PRE)"]
        assert table.events[0].code_suffix == ["const(POST)"]
        assert table.events[1].code_prefix == []


class TestJoinTableConfig:
    def test_join_params_both_on(self) -> None:
        join = JoinTableConfig(path="d_items.csv.gz", both_on=["itemid"])
        assert join.join_params == {"on": ["itemid"]}
        assert join.how == "left"

    def test_join_params_left_right(self) -> None:
        join = JoinTableConfig(
            path="d_items.csv.gz",
            left_on=["itemid"],
            right_on=["item_id"],
            how="inner",
        )
        assert join.join_params == {"left_on": ["itemid"], "right_on": ["item_id"]}
        assert join.how == "inner"
