"""Tests for the KDIGO acute kidney injury transformers.

Scoring unit tests on in-memory frames, in the same shape as the SOFA tests.
What they pin is mostly *which window each rule is measured against* — the
ratio criterion against a 7-day baseline, the absolute rise against 48h, each
urine threshold against its own duration — since those are the parts that look
interchangeable and are not.
"""

from datetime import datetime, timedelta
from typing import cast

import polars as pl
import pytest

from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.config.concept import ConceptConfig
from open_icu.steps.concept.step import ConceptStep
from open_icu.steps.concept.transformer.aki import (
    AkiCreatinineTransformer,
    AkiTransformer,
    AkiUrineOutputTransformer,
)
from open_icu.steps.concept.transformer.windowed import WindowedConceptTransformer

T0 = datetime(2024, 1, 1, 0, 0)


def make(cls: type[WindowedConceptTransformer], **kwargs) -> WindowedConceptTransformer:
    concept = ConceptConfig(name="aki", version="1.0.0", unit="stage")
    config = ComplexDatasetConceptConfig(name="aki", version="1.0", dataset="testdb", concept_transformer="unused")
    return cls(concept, config, cast(ConceptStep, None), **kwargs)


def at(hours: float) -> datetime:
    return T0 + timedelta(hours=hours)


def frame(*rows: tuple[int, datetime, float | None]) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "subject_id": [r[0] for r in rows],
            "time": [r[1] for r in rows],
            "numeric_value": [r[2] for r in rows],
        },
        schema={"subject_id": pl.Int64, "time": pl.Datetime(time_unit="us"), "numeric_value": pl.Float32},
    )


def series(*rows: tuple[float, float]) -> pl.LazyFrame:
    return frame(*[(1, at(hours), value) for hours, value in rows])


def stages(transformer: WindowedConceptTransformer, inputs: dict[str, pl.LazyFrame]) -> dict[float, float]:
    """Stage by hours since T0."""
    out = transformer.transform(inputs).collect().sort("time")
    return {(row[1] - T0).total_seconds() / 3600: row[2] for row in out.iter_rows()}


class TestCreatinineCriterion:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param(1.25, 0.0, id="below-threshold"),  # ratio 1.25 and a rise of 0.25
            pytest.param(1.5, 1.0, id="boundary-1"),
            pytest.param(1.9, 1.0, id="tier-1"),
            pytest.param(2.0, 2.0, id="boundary-2"),
            pytest.param(2.9, 2.0, id="tier-2"),
            pytest.param(3.0, 3.0, id="boundary-3"),
        ],
    )
    def test_ratio_tiers_against_the_baseline(self, value: float, expected: float) -> None:
        # baseline is the 7d minimum, here the admission value of 1.0
        out = stages(make(AkiCreatinineTransformer), {"creatinine": series((0, 1.0), (24, value))})
        assert out[24.0] == expected

    def test_absolute_rise_counts_only_inside_the_acute_window(self) -> None:
        inside = stages(make(AkiCreatinineTransformer), {"creatinine": series((0, 1.0), (40, 1.35))})
        assert inside[40.0] == 1.0  # +0.35 within 48h

        # the same total rise spread over a week: the 48h minimum has moved up with
        # it, and the ratio never reaches 1.5
        slow = stages(
            make(AkiCreatinineTransformer),
            {"creatinine": series((0, 1.0), (50, 1.1), (100, 1.2), (150, 1.35))},
        )
        assert slow[150.0] == 0.0

    def test_stable_chronic_elevation_is_not_injury(self) -> None:
        chronic = {"creatinine": series(*[(hours, 4.5) for hours in range(0, 96, 12)])}
        assert set(stages(make(AkiCreatinineTransformer), chronic).values()) == {0.0}

    def test_the_absolute_threshold_can_be_ungated(self) -> None:
        chronic = {"creatinine": series(*[(hours, 4.5) for hours in range(0, 96, 12)])}
        assert stages(make(AkiCreatinineTransformer, require_acute_rise=False), chronic)[48.0] == 3.0

    def test_renal_replacement_therapy_stages_3(self) -> None:
        out = stages(
            make(AkiCreatinineTransformer, window="24h"),
            {"creatinine": series((0, 1.0), (12, 1.0)), "renal_replacement_therapy": series((6, 1.0))},
        )
        assert out == {0.0: 0.0, 6.0: 3.0, 12.0: 3.0}  # and still 3 while inside the window

    def test_the_therapy_input_is_optional(self) -> None:
        out = stages(make(AkiCreatinineTransformer), {"creatinine": series((0, 1.0), (24, 2.5))})
        assert out[24.0] == 2.0

    def test_baseline_window_is_configurable(self) -> None:
        # small absolute steps, so only the ratio rule can fire: against the 7d
        # minimum of 0.4 the value has trebled-and-a-half; against a 24h minimum
        # of 0.5 it has barely moved
        creatinine = {"creatinine": series((0, 0.4), (48, 0.5), (60, 0.6))}
        assert stages(make(AkiCreatinineTransformer), creatinine)[60.0] == 1.0
        assert stages(make(AkiCreatinineTransformer, baseline_window="24h"), creatinine)[60.0] == 0.0


class TestUrineOutputCriterion:
    """Rates are mL/kg/h; at 70 kg, 35 mL/h is 0.5 and 21 mL/h is 0.3."""

    @pytest.fixture
    def weight(self) -> pl.LazyFrame:
        return series((0, 70.0))

    def test_moderate_oliguria_stages_by_duration(self, weight: pl.LazyFrame) -> None:
        # 0.4 mL/kg/h sustained: stage 1 once 6h is covered, stage 2 once 12h is
        out = stages(
            make(AkiUrineOutputTransformer),
            {"urine_output": series(*[(hours, 28.0) for hours in range(1, 25)]), "patient_weight": weight},
        )
        assert out[7.0] == 1.0
        assert out[13.0] == 2.0

    def test_severe_oliguria_stages_3_over_24h(self, weight: pl.LazyFrame) -> None:
        out = stages(
            make(AkiUrineOutputTransformer),
            {"urine_output": series(*[(hours, 14.0) for hours in range(1, 30)]), "patient_weight": weight},
        )
        assert out[25.0] == 3.0

    def test_anuria_stages_3_over_12h(self, weight: pl.LazyFrame) -> None:
        out = stages(
            make(AkiUrineOutputTransformer),
            {"urine_output": series(*[(hours, 0.0) for hours in range(1, 20)]), "patient_weight": weight},
        )
        assert out[13.0] == 3.0

    def test_an_uncovered_window_is_not_staged(self, weight: pl.LazyFrame) -> None:
        out = stages(
            make(AkiUrineOutputTransformer),
            {"urine_output": series(*[(hours, 28.0) for hours in range(1, 25)]), "patient_weight": weight},
        )
        # before 6h of charting nothing is gradeable -- and that is not stage 0
        assert 5.0 not in out

    def test_a_charting_gap_is_not_oliguria(self, weight: pl.LazyFrame) -> None:
        interrupted = [(hours, 28.0) for hours in range(1, 8)] + [(hours, 28.0) for hours in range(60, 68)]
        out = stages(
            make(AkiUrineOutputTransformer),
            {"urine_output": series(*interrupted), "patient_weight": weight},
        )
        assert 61.0 not in out  # the new segment has not matured
        assert out[67.0] == 1.0  # ...and grades again once it has

    def test_weight_is_required(self) -> None:
        urine = {"urine_output": series(*[(hours, 28.0) for hours in range(1, 25)])}
        assert stages(make(AkiUrineOutputTransformer), urine) == {}

    def test_an_assumed_weight_can_be_configured(self) -> None:
        urine = {"urine_output": series(*[(hours, 28.0) for hours in range(1, 25)])}
        assert stages(make(AkiUrineOutputTransformer, default_weight_kg=70), urine)[7.0] == 1.0

    def test_the_weight_concept_can_be_renamed(self) -> None:
        inputs = {
            "urine_output": series(*[(hours, 28.0) for hours in range(1, 25)]),
            "body_weight": series((0, 70.0)),
        }
        assert stages(make(AkiUrineOutputTransformer, weight_concept="body_weight"), inputs)[7.0] == 1.0


class TestCombinedStage:
    def test_takes_the_higher_criterion(self) -> None:
        out = stages(
            make(AkiTransformer, window="24h"),
            {"aki_creatinine": series((0, 1.0)), "aki_urine_output": series((2, 3.0))},
        )
        assert out[2.0] == 3.0

    def test_a_criterion_outside_the_window_is_skipped_not_zeroed(self) -> None:
        out = stages(
            make(AkiTransformer, window="24h"),
            {"aki_creatinine": series((0, 1.0), (30, 2.0)), "aki_urine_output": series((2, 3.0))},
        )
        assert out[30.0] == 2.0  # the urine stage expired; creatinine is not pulled down to 0

    def test_a_dataset_with_only_one_criterion_still_stages(self) -> None:
        assert stages(make(AkiTransformer, window="24h"), {"aki_creatinine": series((0, 2.0))}) == {0.0: 2.0}
