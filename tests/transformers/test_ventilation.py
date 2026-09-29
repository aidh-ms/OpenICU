from datetime import datetime, timedelta
from typing import cast

import polars as pl

from open_icu import ConceptStep
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.transformer.ventilation import (
    MechanicalVentilationWindowsTransformer,
)

T0 = datetime(2024, 1, 1, 10, 0)


def frame(
    *rows: tuple[int, datetime, float, str] | tuple[int, datetime, float, str, str],
) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "subject_id": [r[0] for r in rows],
            "time": [r[1] for r in rows],
            "numeric_value": [r[2] for r in rows],
            "table": [r[3] for r in rows],
            "stay_id": [r[4] if len(r) == 5 else "1" for r in rows],
        }
    )


def transformer() -> MechanicalVentilationWindowsTransformer:
    concept = ConceptConfig(
        name="mechanical_ventilation_windows",
        version="1.0.0",
        unit="boolean",
    )
    config = ComplexDatasetConceptConfig(
        name="mechanical_ventilation_windows",
        version="1.0",
        dataset="testdb",
        concept_transformer="unused",
    )
    return MechanicalVentilationWindowsTransformer(
        concept,
        config,
        cast(ConceptStep, None),
    )


def times(
    starts: pl.LazyFrame,
    ends: pl.LazyFrame,
    admissions: pl.LazyFrame | None = None,
) -> list[datetime]:
    if admissions is None:
        admissions = frame(
            (1, T0, 1.0, "patient", "1"),
        )

    out = transformer().transform(
        {
            "ventilation_start": starts,
            "ventilation_end": ends,
            "icu_admission": admissions,
        }
    ).collect()

    return out["time"].to_list()


def test_matched_episode_is_expanded_hourly() -> None:
    starts = frame(
        (1, T0 + timedelta(minutes=37), 1.0, "respiratorycharting"),
    )
    ends = frame(
        (1, T0 + timedelta(hours=3), 1.0, "respiratorycare"),
    )

    assert times(starts, ends) == [
        T0,
        T0 + timedelta(hours=1),
        T0 + timedelta(hours=2),
    ]


def test_unmatched_start_defaults_to_six_hours() -> None:
    starts = frame(
        (1, T0, 1.0, "respiratorycharting"),
    )
    ends = frame()

    assert times(starts, ends) == [
        T0 + timedelta(hours=i)
        for i in range(7)
    ]


def test_episode_shorter_than_thirty_minutes_is_dropped() -> None:
    starts = frame(
        (1, T0, 1.0, "respiratorycharting"),
    )
    ends = frame(
        (1, T0 + timedelta(minutes=29), 1.0, "respiratorycare"),
    )

    assert times(starts, ends) == []


def test_exactly_thirty_minutes_is_retained_as_start_hour() -> None:
    starts = frame(
        (1, T0, 1.0, "respiratorycharting"),
    )
    ends = frame(
        (1, T0 + timedelta(minutes=30), 1.0, "respiratorycare"),
    )

    assert times(starts, ends) == [T0]


def test_next_end_is_used() -> None:
    starts = frame(
        (1, T0, 1.0, "respiratorycharting"),
    )
    ends = frame(
        (1, T0 + timedelta(hours=2), 1.0, "respiratorycare"),
        (1, T0 + timedelta(hours=4), 1.0, "respiratorycare"),
    )

    assert times(starts, ends) == [
        T0,
        T0 + timedelta(hours=1),
        T0 + timedelta(hours=2),
    ]


def test_overlapping_episodes_collapse_to_one_hourly_state() -> None:
    starts = frame(
        (1, T0, 1.0, "respiratorycharting"),
        (1, T0 + timedelta(minutes=20), 1.0, "respiratorycharting"),
    )
    ends = frame(
        (1, T0 + timedelta(hours=2), 1.0, "respiratorycare"),
    )

    assert times(starts, ends) == [
        T0,
        T0 + timedelta(hours=1),
        T0 + timedelta(hours=2),
    ]

def test_end_from_different_stay_is_not_matched() -> None:
    starts = frame(
        (1, T0, 1.0, "respiratorycharting", "1"),
    )
    ends = frame(
        (1, T0 + timedelta(hours=2), 1.0, "respiratorycare", "2"),
    )
    admissions = frame(
        (1, T0, 1.0, "patient", "1"),
    )

    assert times(starts, ends, admissions) == [
        T0 + timedelta(hours=i)
        for i in range(7)
    ]


def test_unmatched_duration_includes_start_offset_from_admission() -> None:
    admission = T0
    start = T0 + timedelta(hours=5)

    starts = frame(
        (1, start, 1.0, "respiratorycharting", "1"),
    )
    ends = frame()
    admissions = frame(
        (1, admission, 1.0, "patient", "1"),
    )

    # RICU unmatched duration:
    #   start offset (5h) + match window (6h) = 11h.
    # Expansion is inclusive, hence 12 hourly points.
    assert times(starts, ends, admissions) == [
        start + timedelta(hours=i)
        for i in range(12)
    ]





def test_hour_grid_is_anchored_to_icu_admission() -> None:
    admission = T0 + timedelta(minutes=37)
    start = admission + timedelta(minutes=20)

    starts = frame(
        (1, start, 1.0, "respiratorycharting", "1"),
    )
    ends = frame()
    admissions = frame(
        (1, admission, 1.0, "patient", "1"),
    )

    assert times(starts, ends, admissions) == [
        admission + timedelta(hours=i)
        for i in range(7)
    ]


def test_episode_ending_before_admission_is_expanded_through_hour_zero() -> None:
    admission = T0
    start = admission - timedelta(hours=5)
    end = admission - timedelta(hours=4)

    starts = frame(
        (1, start, 1.0, "respiratorycharting", "1"),
    )
    ends = frame(
        (1, end, 1.0, "respiratorycare", "1"),
    )
    admissions = frame(
        (1, admission, 1.0, "patient", "1"),
    )

    # RICU expand(win_tbl) clamps an interval end below 0 to ICU hour 0.
    assert times(starts, ends, admissions) == [
        admission + timedelta(hours=i)
        for i in range(-5, 1)
    ]
