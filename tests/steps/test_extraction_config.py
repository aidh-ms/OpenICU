"""Tests for extraction step configuration models (table, event, column)."""

import polars as pl

from open_icu.steps.extraction.config.table import JoinTableConfig, TableConfig


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
        assert table.identifier_tuple == ("dataset", "mimic-iv", "3.1", "labevents")
        assert table.identifier == "openicu.config.dataset.mimic-iv.3.1.labevents"


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
