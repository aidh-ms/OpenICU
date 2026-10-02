"""Tests for global event ordering utilities."""

import polars as pl

from open_icu.config.event_order import EventOrderConfig, EventOrderGroup
from open_icu.utils.event_order import sort_events


def test_sort_events_uses_group_order_explicit_order_and_code_fallback() -> None:
    config = EventOrderConfig(
        default_group_order=50,
        unassigned="ignore",
        groups={
            "admission": EventOrderGroup(
                order=10,
                patterns=[r"^HOSPITAL_ADMISSION$"],
            ),
            "labs": EventOrderGroup(
                order=30,
                patterns=[r"^(LAB|albumin|sodium)$"],
            ),
            "derived": EventOrderGroup(
                order=60,
                patterns=[r"^(gcs_|sofa)"],
                explicit_order=[
                    "gcs_eye",
                    "gcs_motor",
                    "gcs_total",
                    "sofa",
                ],
            ),
        },
    )

    lf = pl.DataFrame(
        {
            "subject_id": [1] * 8,
            "time": [None] * 8,
            "code": [
                "sofa//points",
                "sodium//mmol/l",
                "gcs_total//points",
                "albumin//g/dl",
                "gcs_eye//points",
                "HOSPITAL_ADMISSION",
                "LAB//potassium",
                "gcs_motor//points",
            ],
        }
    ).with_columns(
        pl.col("time").cast(pl.Datetime("us"))
    ).lazy()

    result = sort_events(lf, config).collect()

    assert result["code"].to_list() == [
        "HOSPITAL_ADMISSION",
        "LAB//potassium",
        "albumin//g/dl",
        "sodium//mmol/l",
        "gcs_eye//points",
        "gcs_motor//points",
        "gcs_total//points",
        "sofa//points",
    ]


def test_sort_events_uses_first_code_component_for_matching() -> None:
    config = EventOrderConfig(
        default_group_order=50,
        unassigned="ignore",
        groups={
            "labs": EventOrderGroup(
                order=10,
                patterns=[r"^LAB$"],
            ),
        },
    )

    lf = pl.DataFrame(
        {
            "subject_id": [1, 1],
            "time": [None, None],
            "code": [
                "other",
                "LAB//potassium//mmol/l",
            ],
        }
    ).with_columns(
        pl.col("time").cast(pl.Datetime("us"))
    ).lazy()

    result = sort_events(lf, config).collect()

    assert result["code"].to_list() == [
        "LAB//potassium//mmol/l",
        "other",
    ]


def test_event_order_config_matches_first_code_component() -> None:
    config = EventOrderConfig(
        groups={
            "labs": EventOrderGroup(
                order=30,
                patterns=[r"^LAB$"],
            ),
        },
    )

    group_name, _ = config.group_for("LAB//potassium//mmol/l")

    assert group_name == "labs"
    assert config.group_order_for("LAB//potassium//mmol/l") == 30


def test_event_order_uses_first_matching_group() -> None:
    config = EventOrderConfig(
        groups={
            "first": EventOrderGroup(
                order=10,
                patterns=[r"^LAB$"],
            ),
            "second": EventOrderGroup(
                order=20,
                patterns=[r"^LAB$"],
            ),
        },
    )

    group_name, group = config.group_for("LAB//potassium")

    assert group_name == "first"
    assert group is not None
    assert group.order == 10


def test_shipped_event_order_covers_all_concepts() -> None:
    from pathlib import Path

    import yaml

    config = EventOrderConfig.load()

    concept_names = set()

    for path in Path("configs/concepts").rglob("*.yml"):
        data = yaml.safe_load(path.read_text()) or {}
        name = data.get("name")

        if isinstance(name, str):
            concept_names.add(name)

    unassigned = sorted(
        name
        for name in concept_names
        if config.group_for(name)[0] is None
    )

    assert unassigned == []
